import os
import shutil
import tempfile

import pytest

### set environment variables before any app imports
db_fd, db_path = tempfile.mkstemp(prefix="db_")
os.environ["DATABASE_PATH"] = db_path

from gsite import create_app, db
from models.models import User

# Populate test data here


@pytest.fixture
def app():
    """Create and configure a new app instance for each test."""
    # create a temporary file to isolate the database for each test

    app = create_app(
        {
            "TESTING": True,
            "DATA_DIR": f"{os.getcwd()}/tests/data",
        }
    )

    try:
        shutil.rmtree(app.config["TEST_DOWNLOAD_DIR"])
    except:
        pass

    # create the database and load test data
    with app.app_context():
        db.init_db()
        # get_db().executescript(_data_sql)

    yield app

    # close and remove the temporary database
    os.unlink(db_path)


# @pytest.fixture
# def client(app):
#     """A test client for the app."""
#     return app.test_client()
@pytest.yield_fixture
def client(app):
    """A Flask test client. An instance of :class:`flask.testing.TestClient`
    by default.
    """
    with app.test_client() as client:
        yield client


@pytest.fixture
def runner(app):
    """A test runner for the app's Click commands."""
    return app.test_cli_runner()


class AuthActions(object):
    def __init__(self, client):
        self._client = client

    def login(self, username="test", password="test"):
        r = self._client.post("/auth/register", data={"username": username, "password": password})
        assert User.query.filter(User.username == username).one_or_none() is not None

        return r

    def logout(self):
        return self._client.get("/auth/logout")


@pytest.fixture
def auth(client):
    return AuthActions(client)

