from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from .core.config import DATABASE_URL, DB_MAX_OVERFLOW, DB_POOL_RECYCLE, DB_POOL_SIZE

def _resolve_database_url(url: str) -> str:
    if url.startswith("postgresql+"):
        return url
    if not url.startswith("postgresql://"):
        return url
    try:
        import psycopg2  # noqa: F401
        return url
    except Exception:
        pass
    try:
        import psycopg  # noqa: F401
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    except Exception:
        return url


EFFECTIVE_DATABASE_URL = _resolve_database_url(DATABASE_URL)

is_sqlite = EFFECTIVE_DATABASE_URL.startswith("sqlite")
connect_args = {"check_same_thread": False} if is_sqlite else {}

engine_kwargs = {
    "connect_args": connect_args,
    "pool_pre_ping": True,
    "pool_recycle": DB_POOL_RECYCLE,
}
if not is_sqlite:
    engine_kwargs.update(
        {
            "pool_size": DB_POOL_SIZE,
            "max_overflow": DB_MAX_OVERFLOW,
        }
    )

engine = create_engine(EFFECTIVE_DATABASE_URL, **engine_kwargs)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
