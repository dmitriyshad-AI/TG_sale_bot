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
    tallanto_api_token: str = ""
    tallanto_read_only: bool = False
    tallanto_default_contact_module: str = ""
    webapp_dist_path: Path = Path()
    crm_provider: str = "tallanto"
    amo_api_url: str = ""
    amo_access_token: str = ""
    telegram_mode: str = "polling"
    telegram_webhook_secret: str = ""
    telegram_webhook_path: str = "/telegram/webhook"
    admin_miniapp_enabled: bool = False
    admin_telegram_ids: tuple[int, ...] = ()
    admin_webapp_url: str = ""
    user_webapp_url: str = ""
    openai_web_fallback_enabled: bool = True
    openai_web_fallback_domain: str = "kmipt.ru"
    miniapp_brand_name: str = "УНПК МФТИ"
    miniapp_advisor_name: str = "Гид"
    sales_manager_label: str = "Менеджер"
    sales_manager_chat_url: str = ""
    startup_preflight_mode: str = "off"
    running_on_render: bool = False
    persistent_data_root: Path = Path()


def project_root() -> Path:
    # /project_root/sales_agent/sales_core/config.py -> project_root
    return Path(__file__).resolve().parent.parent.parent


def _path_is_writable_directory(path: Path) -> bool:
    return path.exists() and path.is_dir() and os.access(path, os.W_OK)


def get_settings() -> Settings:
    root = project_root()
    running_on_render = any(
        os.getenv(name, "").strip()
        for name in ("RENDER", "RENDER_SERVICE_ID", "RENDER_INSTANCE_ID")
    )
    persistent_data_env = (
        os.getenv("PERSISTENT_DATA_PATH", "").strip()
        or os.getenv("RENDER_PERSISTENT_DATA_PATH", "").strip()
    )
    if persistent_data_env:
        persistent_data_root = Path(persistent_data_env)
    elif running_on_render:
        render_default = Path("/var/data")
        # Render Free may have no mounted persistent disk; keep startup working with /tmp fallback.
        persistent_data_root = render_default if _path_is_writable_directory(render_default) else Path("/tmp")
    else:
        persistent_data_root = Path()
    has_persistent_root = persistent_data_root != Path()

    database_path = os.getenv("DATABASE_PATH")
    db_path = (
        Path(database_path)
        if database_path
        else (
            persistent_data_root / "sales_agent.db"
            if has_persistent_root
            else root / "data" / "sales_agent.db"
        )
    )
    catalog_path = os.getenv("CATALOG_PATH")
    catalog = Path(catalog_path) if catalog_path else root / "catalog" / "products.yaml"
    knowledge_path = os.getenv("KNOWLEDGE_PATH")
    knowledge = Path(knowledge_path) if knowledge_path else root / "knowledge"
    vector_store_meta_path = os.getenv("VECTOR_STORE_META_PATH")
    vector_meta = (
        Path(vector_store_meta_path)
        if vector_store_meta_path
        else (
            persistent_data_root / "vector_store.json"
            if has_persistent_root
            else root / "data" / "vector_store.json"
        )
    )
    webapp_dist_env = os.getenv("WEBAPP_DIST_PATH", "").strip()
    webapp_dist_path = Path(webapp_dist_env) if webapp_dist_env else root / "webapp" / "dist"
    telegram_mode = os.getenv("TELEGRAM_MODE", "polling").strip().lower()
    if telegram_mode not in {"polling", "webhook"}:
        telegram_mode = "polling"
    telegram_webhook_path = os.getenv("TELEGRAM_WEBHOOK_PATH", "/telegram/webhook").strip() or "/telegram/webhook"
    if not telegram_webhook_path.startswith("/"):
        telegram_webhook_path = f"/{telegram_webhook_path}"
    telegram_webhook_secret = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
    admin_miniapp_enabled = os.getenv("ADMIN_MINIAPP_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}
    openai_web_fallback_enabled = (
        os.getenv("OPENAI_WEB_FALLBACK_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
    )
    openai_web_fallback_domain = os.getenv("OPENAI_WEB_FALLBACK_DOMAIN", "kmipt.ru").strip() or "kmipt.ru"
    startup_preflight_mode = os.getenv("STARTUP_PREFLIGHT_MODE", "fail").strip().lower()
    if startup_preflight_mode not in {"off", "fail", "strict"}:
        startup_preflight_mode = "fail"
    tallanto_api_key = os.getenv("TALLANTO_API_KEY", "").strip()
    tallanto_api_token = os.getenv("TALLANTO_API_TOKEN", "").strip() or tallanto_api_key
    tallanto_read_only = os.getenv("TALLANTO_READ_ONLY", "").strip() == "1"
    tallanto_default_contact_module = os.getenv("TALLANTO_DEFAULT_CONTACT_MODULE", "").strip()
    admin_telegram_ids_raw = os.getenv("ADMIN_TELEGRAM_IDS", "").strip()
    admin_telegram_ids: list[int] = []
    if admin_telegram_ids_raw:
        for token in admin_telegram_ids_raw.split(","):
            value = token.strip()
            if not value:
                continue
            try:
                admin_telegram_ids.append(int(value))
            except ValueError:
                continue

    return Settings(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
        telegram_mode=telegram_mode,
        telegram_webhook_secret=telegram_webhook_secret,
        telegram_webhook_path=telegram_webhook_path,
        openai_api_key=os.getenv("OPENAI_API_KEY", "").strip(),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-4.1").strip() or "gpt-4.1",
        tallanto_api_url=os.getenv("TALLANTO_API_URL", "").strip(),
        tallanto_api_key=tallanto_api_key,
        brand_default=os.getenv("BRAND_DEFAULT", "kmipt").strip() or "kmipt",
        database_path=db_path,
        catalog_path=catalog,
        knowledge_path=knowledge,
        vector_store_meta_path=vector_meta,
        openai_vector_store_id=os.getenv("OPENAI_VECTOR_STORE_ID", "").strip(),
        openai_web_fallback_enabled=openai_web_fallback_enabled,
        openai_web_fallback_domain=openai_web_fallback_domain,
        admin_user=os.getenv("ADMIN_USER", "").strip(),
        admin_pass=os.getenv("ADMIN_PASS", "").strip(),
        tallanto_api_token=tallanto_api_token,
        tallanto_read_only=tallanto_read_only,
        tallanto_default_contact_module=tallanto_default_contact_module,
        webapp_dist_path=webapp_dist_path,
        crm_provider=os.getenv("CRM_PROVIDER", "none").strip().lower() or "none",
        amo_api_url=os.getenv("AMO_API_URL", "").strip(),
        amo_access_token=os.getenv("AMO_ACCESS_TOKEN", "").strip(),
        admin_miniapp_enabled=admin_miniapp_enabled,
        admin_telegram_ids=tuple(admin_telegram_ids),
        admin_webapp_url=os.getenv("ADMIN_WEBAPP_URL", "").strip(),
        user_webapp_url=os.getenv("USER_WEBAPP_URL", "").strip(),
        miniapp_brand_name=os.getenv("MINIAPP_BRAND_NAME", "УНПК МФТИ").strip() or "УНПК МФТИ",
        miniapp_advisor_name=os.getenv("MINIAPP_ADVISOR_NAME", "Гид").strip() or "Гид",
        sales_manager_label=os.getenv("SALES_MANAGER_LABEL", "Менеджер").strip() or "Менеджер",
        sales_manager_chat_url=os.getenv("SALES_MANAGER_CHAT_URL", "").strip(),
        startup_preflight_mode=startup_preflight_mode,
        running_on_render=running_on_render,
        persistent_data_root=persistent_data_root,
    )
