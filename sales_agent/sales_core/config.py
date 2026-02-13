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
    catalog_path: Path
    knowledge_path: Path
    vector_store_meta_path: Path
    openai_vector_store_id: str
    admin_user: str
    admin_pass: str
    crm_provider: str = "tallanto"
    amo_api_url: str = ""
    amo_access_token: str = ""


def project_root() -> Path:
    # /project_root/sales_agent/sales_core/config.py -> project_root
    return Path(__file__).resolve().parent.parent.parent


def get_settings() -> Settings:
    root = project_root()
    database_path = os.getenv("DATABASE_PATH")
    db_path = Path(database_path) if database_path else root / "data" / "sales_agent.db"
    catalog_path = os.getenv("CATALOG_PATH")
    catalog = Path(catalog_path) if catalog_path else root / "catalog" / "products.yaml"
    knowledge_path = os.getenv("KNOWLEDGE_PATH")
    knowledge = Path(knowledge_path) if knowledge_path else root / "knowledge"
    vector_store_meta_path = os.getenv("VECTOR_STORE_META_PATH")
    vector_meta = (
        Path(vector_store_meta_path)
        if vector_store_meta_path
        else root / "data" / "vector_store.json"
    )
    return Settings(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
        openai_api_key=os.getenv("OPENAI_API_KEY", "").strip(),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-4.1").strip() or "gpt-4.1",
        tallanto_api_url=os.getenv("TALLANTO_API_URL", "").strip(),
        tallanto_api_key=os.getenv("TALLANTO_API_KEY", "").strip(),
        brand_default=os.getenv("BRAND_DEFAULT", "kmipt").strip() or "kmipt",
        database_path=db_path,
        catalog_path=catalog,
        knowledge_path=knowledge,
        vector_store_meta_path=vector_meta,
        openai_vector_store_id=os.getenv("OPENAI_VECTOR_STORE_ID", "").strip(),
        admin_user=os.getenv("ADMIN_USER", "").strip(),
        admin_pass=os.getenv("ADMIN_PASS", "").strip(),
        crm_provider=os.getenv("CRM_PROVIDER", "tallanto").strip().lower() or "tallanto",
        amo_api_url=os.getenv("AMO_API_URL", "").strip(),
        amo_access_token=os.getenv("AMO_ACCESS_TOKEN", "").strip(),
    )
