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
    bytes_type = "BIGINT" if engine.dialect.name == "postgresql" else "INTEGER"

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

    if "game_graphics_configs" in tables:
        columns = {col["name"] for col in inspector.get_columns("game_graphics_configs")}
        alters = []
        if "dx12_flags" not in columns:
            alters.append(f"ALTER TABLE game_graphics_configs ADD COLUMN dx12_flags {json_type}")
        if "dx11_flags" not in columns:
            alters.append(f"ALTER TABLE game_graphics_configs ADD COLUMN dx11_flags {json_type}")
        if "vulkan_flags" not in columns:
            alters.append(f"ALTER TABLE game_graphics_configs ADD COLUMN vulkan_flags {json_type}")
        if "overlay_enabled" not in columns:
            alters.append(
                f"ALTER TABLE game_graphics_configs ADD COLUMN overlay_enabled BOOLEAN DEFAULT {_bool_default(True)}"
            )
        if "recommended_api" not in columns:
            alters.append("ALTER TABLE game_graphics_configs ADD COLUMN recommended_api VARCHAR(20)")
        if "executable" not in columns:
            alters.append("ALTER TABLE game_graphics_configs ADD COLUMN executable VARCHAR(260)")
        if "game_dir" not in columns:
            alters.append("ALTER TABLE game_graphics_configs ADD COLUMN game_dir VARCHAR(260)")
        if "created_at" not in columns:
            if timestamp_default:
                alters.append(
                    f"ALTER TABLE game_graphics_configs ADD COLUMN created_at {timestamp_type} DEFAULT {timestamp_default}"
                )
            else:
                alters.append(f"ALTER TABLE game_graphics_configs ADD COLUMN created_at {timestamp_type}")
        if "updated_at" not in columns:
            if timestamp_default:
                alters.append(
                    f"ALTER TABLE game_graphics_configs ADD COLUMN updated_at {timestamp_type} DEFAULT {timestamp_default}"
                )
            else:
                alters.append(f"ALTER TABLE game_graphics_configs ADD COLUMN updated_at {timestamp_type}")
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

    if "user_profiles" in tables:
        columns = {col["name"] for col in inspector.get_columns("user_profiles")}
        alters = []
        if "background_image" not in columns:
            alters.append("ALTER TABLE user_profiles ADD COLUMN background_image VARCHAR(500)")
        _apply_alters(alters)

    if "download_tasks" in tables:
        columns = {col["name"] for col in inspector.get_columns("download_tasks")}
        alters = []
        if "downloaded_bytes" not in columns:
            alters.append(
                f"ALTER TABLE download_tasks ADD COLUMN downloaded_bytes {bytes_type} DEFAULT 0"
            )
        if "total_bytes" not in columns:
            alters.append(f"ALTER TABLE download_tasks ADD COLUMN total_bytes {bytes_type} DEFAULT 0")
        if "network_bps" not in columns:
            alters.append(f"ALTER TABLE download_tasks ADD COLUMN network_bps {bytes_type} DEFAULT 0")
        if "disk_read_bps" not in columns:
            alters.append(f"ALTER TABLE download_tasks ADD COLUMN disk_read_bps {bytes_type} DEFAULT 0")
        if "disk_write_bps" not in columns:
            alters.append(f"ALTER TABLE download_tasks ADD COLUMN disk_write_bps {bytes_type} DEFAULT 0")
        if "read_bytes" not in columns:
            alters.append(f"ALTER TABLE download_tasks ADD COLUMN read_bytes {bytes_type} DEFAULT 0")
        if "written_bytes" not in columns:
            alters.append(f"ALTER TABLE download_tasks ADD COLUMN written_bytes {bytes_type} DEFAULT 0")
        if "remaining_bytes" not in columns:
            alters.append(
                f"ALTER TABLE download_tasks ADD COLUMN remaining_bytes {bytes_type} DEFAULT 0"
            )
        _apply_alters(alters)

    if "game_play_sessions" not in tables:
        started_default = f"DEFAULT {timestamp_default}" if timestamp_default else ""
        created_default = f"DEFAULT {timestamp_default}" if timestamp_default else ""
        updated_default = f"DEFAULT {timestamp_default}" if timestamp_default else ""
        create_statement = f"""
            CREATE TABLE game_play_sessions (
                id VARCHAR(36) PRIMARY KEY,
                user_id VARCHAR(36) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                game_id VARCHAR(36) NOT NULL REFERENCES games(id) ON DELETE CASCADE,
                started_at {timestamp_type} NOT NULL {started_default},
                ended_at {timestamp_type},
                duration_sec INTEGER DEFAULT 0,
                exit_code INTEGER,
                created_at {timestamp_type} {created_default},
                updated_at {timestamp_type} {updated_default}
            )
        """
        with engine.begin() as connection:
            connection.execute(text(create_statement))
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_game_play_sessions_user_id "
                    "ON game_play_sessions (user_id)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_game_play_sessions_game_id "
                    "ON game_play_sessions (game_id)"
                )
            )


def _apply_alters(statements: list[str]) -> None:
    if not statements:
        return
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))
