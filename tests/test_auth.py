import pytest
from flask import g, session

from gsite import db
from models.models import User


def test_register(client, app):
    # test that viewing the page renders without template errors
    assert client.get("/auth/register").status_code == 200

    # test that successful registration redirects to the login page
    response = client.post(
        "/auth/register", data={"username": "a", "email": "a@b.c", "password": "a"}
    )

    assert "http://localhost/" == response.headers["Location"]

    # test that the user was inserted into the database
    with app.app_context():
        assert User.query.filter(User.username == "a") is not None

