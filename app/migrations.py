from sqlalchemy import inspect, text

from .db import engine


def _bool_default(value: bool) -> str:
    if engine.dialect.name == "postgresql":
        return "TRUE" if value else "FALSE"
    return "1" if value else "0"


def _json_type() -> str:
    return "JSONB" if engine.dialect.name == "postgresql" else "TEXT"


def _now_default() -> str:
    return "CURRENT_TIMESTAMP"


def ensure_schema() -> None:
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    json_type = _json_type()
    timestamp_type = "TIMESTAMP" if engine.dialect.name == "postgresql" else "DATETIME"
    timestamp_default = _now_default() if engine.dialect.name == "postgresql" else None

    if "users" in tables:
        columns = {col["name"] for col in inspector.get_columns("users")}
        alters = []
        if "avatar_url" not in columns:
            alters.append("ALTER TABLE users ADD COLUMN avatar_url VARCHAR(500)")
        if "xp" not in columns:
            alters.append("ALTER TABLE users ADD COLUMN xp INTEGER DEFAULT 0")
        if "level" not in columns:
            alters.append("ALTER TABLE users ADD COLUMN level INTEGER DEFAULT 1")
        if "wallet_balance" not in columns:
            alters.append("ALTER TABLE users ADD COLUMN wallet_balance REAL DEFAULT 0")
        if "last_login" not in columns:
            alters.append(f"ALTER TABLE users ADD COLUMN last_login {timestamp_type}")
        if "updated_at" not in columns:
            if timestamp_default:
                alters.append(
                    f"ALTER TABLE users ADD COLUMN updated_at {timestamp_type} DEFAULT {timestamp_default}"
                )
            else:
                alters.append(f"ALTER TABLE users ADD COLUMN updated_at {timestamp_type}")
        if "is_active" not in columns:
            alters.append(f"ALTER TABLE users ADD COLUMN is_active BOOLEAN DEFAULT {_bool_default(True)}")
        if "is_verified" not in columns:
            alters.append(f"ALTER TABLE users ADD COLUMN is_verified BOOLEAN DEFAULT {_bool_default(False)}")
        if "role" not in columns:
            alters.append("ALTER TABLE users ADD COLUMN role VARCHAR(20) DEFAULT 'user'")
        _apply_alters(alters)

    if "games" in tables:
        columns = {col["name"] for col in inspector.get_columns("games")}
        alters = []
        if "short_description" not in columns:
            alters.append("ALTER TABLE games ADD COLUMN short_description VARCHAR(300)")
        if "developer" not in columns:
            alters.append("ALTER TABLE games ADD COLUMN developer VARCHAR(120)")
        if "publisher" not in columns:
            alters.append("ALTER TABLE games ADD COLUMN publisher VARCHAR(120)")
        if "background_image" not in columns:
            alters.append("ALTER TABLE games ADD COLUMN background_image VARCHAR(500)")
        if "tags" not in columns:
            alters.append(f"ALTER TABLE games ADD COLUMN tags {json_type}")
        if "platforms" not in columns:
            alters.append(f"ALTER TABLE games ADD COLUMN platforms {json_type}")
        if "screenshots" not in columns:
            alters.append(f"ALTER TABLE games ADD COLUMN screenshots {json_type}")
        if "videos" not in columns:
            alters.append(f"ALTER TABLE games ADD COLUMN videos {json_type}")
        if "system_requirements" not in columns:
            alters.append(f"ALTER TABLE games ADD COLUMN system_requirements {json_type}")
        if "total_downloads" not in columns:
            alters.append("ALTER TABLE games ADD COLUMN total_downloads INTEGER DEFAULT 0")
        if "average_rating" not in columns:
            alters.append("ALTER TABLE games ADD COLUMN average_rating REAL DEFAULT 0")
        if "updated_at" not in columns:
            if timestamp_default:
                alters.append(
                    f"ALTER TABLE games ADD COLUMN updated_at {timestamp_type} DEFAULT {timestamp_default}"
                )
            else:
                alters.append(f"ALTER TABLE games ADD COLUMN updated_at {timestamp_type}")
        _apply_alters(alters)


def _apply_alters(statements: list[str]) -> None:
    if not statements:
        return
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))
