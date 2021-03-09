import os

import werkzeug
from flask import Flask, flash, jsonify, redirect, url_for
from flask_admin import Admin
from flask_security import (
    Security,
    SQLAlchemySessionUserDatastore,
    login_required,
    roles_required,
)

if not "DATABASE_PATH" in os.environ:
    os.environ["DATABASE_PATH"] = "instance/site.sql"
if not "COC_AUTH_TOKEN" in os.environ:
    os.environ[
        "COC_AUTH_TOKEN"
    ] = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiIsImtpZCI6IjI4YTMxOGY3LTAwMDAtYTFlYi03ZmExLTJjNzQzM2M2Y2NhNSJ9.eyJpc3MiOiJzdXBlcmNlbGwiLCJhdWQiOiJzdXBlcmNlbGw6Z2FtZWFwaSIsImp0aSI6IjQ4OTgyYzExLTNiNTItNGFmMi04Yjc3LTkzNTAzZDg1ZDYyOCIsImlhdCI6MTU2Mjc3Njk0OCwic3ViIjoiZGV2ZWxvcGVyLzI4MGM2NzBhLWJhYjUtNDMzNC1mOTU0LTA5Njc0YjgxZmM2MCIsInNjb3BlcyI6WyJjbGFzaCJdLCJsaW1pdHMiOlt7InRpZXIiOiJkZXZlbG9wZXIvc2lsdmVyIiwidHlwZSI6InRocm90dGxpbmcifSx7ImNpZHJzIjpbIjM1LjE3My42OS4yMDciLCIxMzYuNDkuNzguODkiXSwidHlwZSI6ImNsaWVudCJ9XX0.jx_wj0cZ813FL3LlS0OuRew8v7l-Ra25r08WxUUOrf0VxG2ETp_Vs_u-PEx5QvQXYRSS-JKSsKshDC0pm527yg"
from gsite.auth import __hash_password__, require_login

# dotenv_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
# if os.path.exists(dotenv_file):
#     import dotenv
#     dotenv.load_dotenv(dotenv_file)


def has_no_empty_params(rule):
    defaults = rule.defaults if rule.defaults is not None else ()
    arguments = rule.arguments if rule.arguments is not None else ()
    return len(defaults) >= len(arguments)


DEFAULT_OPTIONS = {
    "DEFAULT_ADMIN_PASSWORD": "Wn83xfGH2sxZrg5D",
    "SECRET_KEY": "3en934kyex58rkpen",
    "DEBUG_LEVEL": "1",
}


def create_app(test_config=None):
    """Create and configure an instance of the Flask application."""
    app = Flask(__name__, instance_relative_config=True)

    app.config.from_mapping(DEFAULT_OPTIONS)

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile("config.py")
    else:
        # load the test config if passed in
        app.config.update(test_config)

    ### check for necessary configurations
    asserts = []
    for a in asserts:
        assert a in app.config, f"Must specify '{a}' inside of the config.py"
        print(f"Config Variable: {a}={app.config[a]}")

    asserts = ["SQLALCHEMY_DATABASE_URI"]
    for a in asserts:
        assert a in os.environ, f"Must specify '{a}' in your environment variables"
        print(f"Environment Variable: {a}={os.environ[a]}")

    # ensure the needed folders exist
    folders = ["logs", "instance/data"]
    os.makedirs(app.instance_path, exist_ok=True)
    for d in folders:
        os.makedirs(d, exist_ok=True)

    # register commands
    import models.models as models
    from gsite import db

    db.init_app(app)

    import gsite.plots as plots

    plots.init_app(app)

    import gsite.table as table

    table.init_app(app)

    @app.before_first_request
    def before_first_request():
        admin,__ = db.get_or_create(
            db.session,
            models.User,
            filter_kwargs={"username": "admin"},
            create_kwargs={
                "username": "admin",
                "email": "admin@generic-website.com",
                "password": __hash_password__(app.config["DEFAULT_ADMIN_PASSWORD"]),
            },
        )
        role,__ = db.get_or_create(
            db.session,
            models.Role,
            filter_kwargs={"name": "admin"},
            create_kwargs={"name": "admin", "description": "Administrator"},
        )

        if not admin.has_role(role):
            admin.roles.append(role)

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        db.session.remove()

    from sqlalchemy.exc import DatabaseError

    @app.after_request
    def session_commit(response):
        if response.status_code >= 400:
            return response
        try:
            db.session.commit()
        except DatabaseError:
            db.session.rollback()
            raise
        return response

    # apply the blueprints to the app
    from gsite import auth, main

    app.register_blueprint(auth.bp)
    app.register_blueprint(main.bp)

    app.add_url_rule("/", endpoint="index")

    from gsite import logger

    @app.errorhandler(logger.HtmlException)
    def handle_html_exception(error):
        response = jsonify(error.to_dict())
        response.status_code = error.status_code
        logger.stacktrace(str(error))
        if error.url_redirect:
            flash(error.message)
            return redirect(url_for(error.url_redirect))

        return response

    @app.errorhandler(werkzeug.exceptions.Forbidden)
    def handle_forbidden_error(error):
        logger.stacktrace(str(error), error)
        flash("You do not have sufficient privileges for this resource")
        return redirect(url_for("site.index"))

    @app.errorhandler(Exception)
    def handle_generic_error(error):
        if isinstance(error, werkzeug.exceptions.NotFound):
            return error
        response = jsonify({})
        response.status_code = 403
        logger.stacktrace(str(error), error)
        return response

    @app.before_request
    def validate_logins():
        return require_login()

    @app.route("/site-map")
    @roles_required("admin")
    def site_map():
        links = []
        for rule in app.url_map.iter_rules():
            # Filter out rules we can't navigate to in a browser
            # and rules that require parameters
            if "GET" in rule.methods and has_no_empty_params(rule):
                url = url_for(rule.endpoint, **(rule.defaults or {}))
                links.append((url, rule.endpoint))
        return jsonify(links)

    ### Handle Flask Admin
    app.config["FLASK_ADMIN_SWATCH"] = "cerulean"

    from flask_admin.base import AdminIndexView
    from flask_admin.contrib.sqla import ModelView
    from flask_security import current_user

    class MyHomeView(AdminIndexView):
        def is_accessible(self):
            return current_user.is_admin()

    class RoleAdmin(ModelView):
        def is_accessible(self):
            return current_user.is_admin()

    admin = Admin(app, name="Site Admin", template_mode="bootstrap3", index_view=MyHomeView())
    admin.add_view(RoleAdmin(models.User, db.session))
    admin.add_view(RoleAdmin(models.Role, db.session))

    # Setup Flask-Security
    user_datastore = SQLAlchemySessionUserDatastore(db.session, models.User, models.Role)
    security = Security(app, user_datastore)
    if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        import runner
        runner.run_legends()
    print("Finished init")
    return app


app = create_app()
