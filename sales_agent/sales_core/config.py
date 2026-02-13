import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from a local .env if present.
load_dotenv()


@dataclass
class Settings:
    telegram_bot_token: str
    openai_api_key: str
    openai_model: str
    tallanto_api_url: str
    tallanto_api_key: str
    brand_default: str
    database_path: Path


def project_root() -> Path:
    # /project_root/sales_agent/sales_core/config.py -> project_root
    return Path(__file__).resolve().parent.parent.parent


def get_settings() -> Settings:
    root = project_root()
    database_path = os.getenv("DATABASE_PATH")
    db_path = Path(database_path) if database_path else root / "data" / "sales_agent.db"
    return Settings(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
        openai_api_key=os.getenv("OPENAI_API_KEY", "").strip(),
        openai_model=os.getenv("OPENAI_MODEL", "").strip(),
        tallanto_api_url=os.getenv("TALLANTO_API_URL", "").strip(),
        tallanto_api_key=os.getenv("TALLANTO_API_KEY", "").strip(),
        brand_default=os.getenv("BRAND_DEFAULT", "kmipt").strip() or "kmipt",
        database_path=db_path,
    )

