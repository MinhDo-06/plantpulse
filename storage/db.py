import functools
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

ENGINE = create_engine(os.getenv("DATABASE_URL", "mysql+pymysql://Minh:whitesox123@localhost:3306/weather"))


def make_session():
    return sessionmaker(bind=ENGINE)()


def use_db_session(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        session = make_session()
        try:
            return func(session, *args, **kwargs)
        finally:
            session.close()

    return wrapper
