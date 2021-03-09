import functools

import werkzeug
from flask import (
    Blueprint,
    flash,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from models import db
from models.models import User

bp = Blueprint("auth", __name__, url_prefix="/auth")

VALID_EMAILS = set(["alkarin.v@gmail.com"])
### These are resources that don't need logins the "None" or / and the static folder
_insecure_views = set(["None", "static"])


def require_login():
    if request.endpoint in _insecure_views:
        return

    if g.user is None:
        flash("Please login to access %s" % request.endpoint)
        return redirect(url_for("auth.login"))


def login_not_required(bp):
    def login_required_func(func):
        endpoint = ".".join([bp.name, func.__name__])
        _insecure_views.add(endpoint)

    return login_required_func


@bp.before_app_request
def load_logged_in_user():
    """If a user id is stored in the session, load the user object from
    the database into ``g.user``."""
    user_id = session.get("user_id")
    if user_id is None:
        g.user = None
    else:
        g.user = db.session.query(User).filter(User.id == user_id).one_or_none()

def __hash_password__(password):
    return werkzeug.security.generate_password_hash(password)

def __check_hash__(password, potential_password):
    return werkzeug.security.check_password_hash(password, potential_password)

@login_not_required(bp)
@bp.route("/register", methods=("GET", "POST"))
def register():
    """Register a new user.

    Validates that the username is not already taken. Hashes the
    password for security.
    """
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        email = request.form["email"]

        error = None

        if not username:
            error = "Username is required."
        elif not email:
            error = "Email is required."
        elif not password:
            error = "Password is required."
        elif User.query.filter(User.username == username).one_or_none() is not None:
            error = "User {0} is already registered.".format(username)
        elif email not in VALID_EMAILS:
            error = "Email {0} is not in the whitelist.".format(username)
        if error is None:
            # the name is available, store it in the database and go to
            # the login page
            u = User(username, email, __hash_password__(password))
            db.session.add(u)
            db.session.commit()
            db.session.refresh(u)
            session["user_id"] = u.id
            g.user = u
            return redirect(url_for("index"))
        flash(error)

    return render_template("auth/register.html")


@login_not_required(bp)
@bp.route("/login", methods=("GET", "POST"))
def login():
    """Log in a registered user by adding the user id to the session."""
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        error = None
        users = User.query.all()
        user = User.query.filter(User.username == username).one_or_none()
        if not user:
            user = User.query.filter(User.email == username).one_or_none()

        if user is None:
            error = "Incorrect username."
        elif not __check_hash__(user.password, password):
            error = "Incorrect password."

        if error is None:
            # store the user id in a new session and return to the index
            session.clear()
            session["user_id"] = user.id
            g.user = db.session.query(User).filter(User.id == user.id).one_or_none()
            return redirect(url_for("index"))

        flash(error)

    return render_template("auth/login.html")


@login_not_required(bp)
@bp.route("/logout")
def logout():
    """Clear the current session, including the stored user id."""
    session.clear()
    return redirect(url_for("index"))
