from flask_security import RoleMixin
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import backref, relationship

from models.db import Base as DBBase


class User(DBBase):
    __tablename__ = "user"
    __table_args__ = {"extend_existing": True}
    id = Column(Integer, primary_key=True)
    username = Column(String(64), unique=True)
    email = Column(String(256), nullable=False)
    password = Column(String(256), nullable=False)
    roles = relationship("Role", secondary="roles_users", backref=backref("users", lazy="dynamic"))

    def __init__(self, username=None, email=None, password=None):
        self.username = username
        self.email = email
        self.password = password

    def __str__(self):
        return self.username

    def __repr__(self):
        return "<User %r %r>" % (self.username, self.email)

    def is_admin(self):
        return self.has_role("admin")

    def has_role(self, role):
        if isinstance(role, Role):
            return any(r == role for r in self.roles)
        return any(r.name == role for r in self.roles)


class RolesUsers(DBBase):
    __tablename__ = "roles_users"
    id = Column(Integer(), primary_key=True)
    user_id = Column("user_id", Integer(), ForeignKey("user.id"))
    role_id = Column("role_id", Integer(), ForeignKey("role.id"))


class Role(DBBase, RoleMixin):
    __tablename__ = "role"
    id = Column(Integer(), primary_key=True)
    name = Column(String(80), unique=True)
    description = Column(String(255))

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<Role %r %r>" % (self.name, self.description)

    def __eq__(self, other):
        """
            There can not be duplicate names,
            and duplicate names should be considered the same
        """
        if not isinstance(other, Role):
            return NotImplemented
        return self.id == other.id or self.name.lower() == other.name.lower()

