import os
from enum import IntEnum

# from flask.cli import with_appcontext
from sqlalchemy import create_engine
from sqlalchemy.exc import DatabaseError, IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.orm.exc import NoResultFound

# import gsite.logger as log
x = os.getenv("SQLALCHEMY_DATABASE_URI")
if not x:
    raise IOError("No Database connection. Set 'SQLALCHEMY_DATABASE_URI' or see Readme. ")
print(f"SQLALCHEMY_DATABASE_URI = {x}")
engine = create_engine(os.getenv("SQLALCHEMY_DATABASE_URI"))
session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

class Driver(IntEnum):
    mysql = 0
    sqlite = 1

    @staticmethod
    def from_drivername(drivername):
        if drivername == "sqlite":
            return Driver.sqlite
        else:
            return Driver.mysql
    def is_sqlite(self):
        return self == Driver.sqlite

driver = Driver.from_drivername(session.bind.url.drivername)


def get_one_or_create(session, model, create_method="", create_method_kwargs=None, **kwargs):
    try:
        return session.query(model).filter_by(**kwargs).one(), False
    except NoResultFound:
        kwargs.update(create_method_kwargs or {})
        created = getattr(model, create_method, model)(**kwargs)
        try:
            session.add(created)
            session.flush()
            return created, True
        except IntegrityError:
            session.rollback()
            return session.query(model).filter_by(**kwargs).one(), False

def get(session, model, filter_kwargs):
    try:
        return session.query(model).filter_by(**filter_kwargs).one()
    except NoResultFound:
        return None

def get_or_create(session, model, filter_kwargs, create_kwargs, flush=True):
    try:
        return session.query(model).filter_by(**filter_kwargs).one(), False
    except NoResultFound:
        try:
            instance = model(**create_kwargs)
            session.add(instance)
            if flush:
                session.flush()
            return instance, True
        except IntegrityError:
            session.rollback()
            return session.query(model).filter_by(**filter_kwargs).one(), False


def __add_and_refresh(self, instance, _warn=True):
    self.add(instance, _warn)
    self.commit()
    self.refresh(instance)
    pk = inspect(instance).identity
    return pk


setattr(session.__class__, "add_and_refresh", __add_and_refresh)


class DeclarativeBase(object):
    def as_json(self):
        return {k: str(v) for k, v in vars(self).items() if not k.startswith("_")}

    def as_dict(self):
        return {k: str(v) for k, v in vars(self).items() if not k.startswith("_")}

    def eq_dict(self):
        return {k: str(v) for k, v in vars(self).items() if not k.startswith("_") and k in self.__equality_attrs__}


    @classmethod
    def from_dict(cls, d):
        c = cls()
        for k, v in d.items():
            setattr(c, k, v)
        return c

    def save(self):
        session.add(self)
        self._flush()
        return self

    def merge(self):
        session.merge(self)
        self._flush()
        return self

    def update(self, **kwargs):
        for attr, value in kwargs.items():
            setattr(self, attr, value)
        return self.save()

    def delete(self):
        session.delete(self)
        self._flush()

    def _flush(self):
        try:
            session.flush()
        except DatabaseError:
            session.rollback()
            raise

    def __init__(self, **kwargs):
        for attr, value in kwargs.items():
            setattr(self, attr, value)


Base = declarative_base(cls=DeclarativeBase)
Base.query = session.query_property()


def init_db():
    """Init the db, create the models"""
    import models.models

    Base.metadata.create_all(bind=engine)


# @click.command("init-db")
# @with_appcontext
# def init_db_command():
#     """Clear existing data and create new tables."""
#     init_db()
#     click.echo("Initialized the database.")


def init_app(app):
    """Register database functions with the Flask app. This is called by
    the application factory.
    """
    app.cli.add_command(init_db_command)


def execute(query, args={}, one=False):
    # log.printl(query, args)
    result = session.execute(query, args)
    return (result.first() if result else None) if one else result

