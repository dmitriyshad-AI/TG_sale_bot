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
    app_env: str = "development"
    startup_preflight_mode: str = "off"
    running_on_render: bool = False
    persistent_data_root: Path = Path()
    rate_limit_backend: str = "memory"
    redis_url: str = ""
    admin_ui_csrf_enabled: bool = False
    assistant_api_token: str = ""
    assistant_rate_limit_window_seconds: int = 60
    assistant_rate_limit_user_requests: int = 24
    assistant_rate_limit_ip_requests: int = 72
    crm_api_exposed: bool = False
    crm_rate_limit_window_seconds: int = 300
    crm_rate_limit_ip_requests: int = 180
    enable_business_inbox: bool = False
    enable_call_copilot: bool = False
    enable_tallanto_enrichment: bool = False
    enable_director_agent: bool = False
    enable_lead_radar: bool = False
    enable_faq_lab: bool = False
    enable_mango_auto_ingest: bool = False
    lead_radar_scheduler_enabled: bool = True
    lead_radar_interval_seconds: int = 3600
    lead_radar_no_reply_hours: int = 6
    lead_radar_call_no_next_step_hours: int = 24
    lead_radar_stale_warm_days: int = 7
    lead_radar_max_items_per_run: int = 50
    lead_radar_thread_cooldown_hours: int = 24
    lead_radar_daily_cap_per_thread: int = 2
    faq_lab_scheduler_enabled: bool = True
    faq_lab_interval_seconds: int = 21600
    faq_lab_window_days: int = 90
    faq_lab_min_question_count: int = 2
    faq_lab_max_items_per_run: int = 120
    mango_api_base_url: str = ""
    mango_api_token: str = ""
    mango_calls_path: str = "/calls"
    mango_webhook_path: str = "/integrations/mango/webhook"
    mango_webhook_secret: str = ""
    mango_polling_enabled: bool = False
    mango_poll_interval_seconds: int = 300
    mango_call_recording_ttl_hours: int = 48
    mango_poll_limit_per_run: int = 50
    mango_poll_retry_attempts: int = 3
    mango_poll_retry_backoff_seconds: int = 2
    mango_retry_failed_limit_per_run: int = 25


def project_root() -> Path:
    # /project_root/sales_agent/sales_core/config.py -> project_root
    return Path(__file__).resolve().parent.parent.parent


def _path_is_writable_directory(path: Path) -> bool:
    return path.exists() and path.is_dir() and os.access(path, os.W_OK)


def get_settings() -> Settings:
    root = project_root()

    def _parse_bool_env(name: str, default: bool = False) -> bool:
        raw = os.getenv(name, "").strip().lower()
        if not raw:
            return default
        return raw in {"1", "true", "yes", "on"}

    def _parse_int_env(name: str, default: int, *, min_value: int, max_value: int) -> int:
        raw = os.getenv(name, "").strip()
        if not raw:
            return default
        try:
            parsed = int(raw)
        except ValueError:
            return default
        return max(min_value, min(max_value, parsed))

    running_on_render = any(
        os.getenv(name, "").strip()
        for name in ("RENDER", "RENDER_SERVICE_ID", "RENDER_INSTANCE_ID")
    )
    persistent_data_env = (
        os.getenv("PERSISTENT_DATA_PATH", "").strip()
        or os.getenv("RENDER_DISK_MOUNT_PATH", "").strip()
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
    crm_api_exposed = _parse_bool_env("CRM_API_EXPOSED", default=False)
    openai_web_fallback_enabled = (
        os.getenv("OPENAI_WEB_FALLBACK_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
    )
    openai_web_fallback_domain = os.getenv("OPENAI_WEB_FALLBACK_DOMAIN", "kmipt.ru").strip() or "kmipt.ru"
    startup_preflight_mode = os.getenv("STARTUP_PREFLIGHT_MODE", "fail").strip().lower()
    if startup_preflight_mode not in {"off", "fail", "strict"}:
        startup_preflight_mode = "fail"
    app_env = os.getenv("APP_ENV", "development").strip().lower()
    if app_env not in {"development", "staging", "production"}:
        app_env = "development"
    rate_limit_backend = os.getenv("RATE_LIMIT_BACKEND", "memory").strip().lower()
    if rate_limit_backend not in {"memory", "redis"}:
        rate_limit_backend = "memory"
    redis_url = os.getenv("REDIS_URL", "").strip()
    admin_ui_csrf_enabled = _parse_bool_env(
        "ADMIN_UI_CSRF_ENABLED",
        default=(app_env == "production"),
    )
    assistant_rate_limit_window_seconds = _parse_int_env(
        "ASSISTANT_RATE_LIMIT_WINDOW_SECONDS",
        60,
        min_value=10,
        max_value=3600,
    )
    assistant_rate_limit_user_requests = _parse_int_env(
        "ASSISTANT_RATE_LIMIT_USER_REQUESTS",
        24,
        min_value=1,
        max_value=1000,
    )
    assistant_rate_limit_ip_requests = _parse_int_env(
        "ASSISTANT_RATE_LIMIT_IP_REQUESTS",
        72,
        min_value=1,
        max_value=5000,
    )
    crm_rate_limit_window_seconds = _parse_int_env(
        "CRM_RATE_LIMIT_WINDOW_SECONDS",
        300,
        min_value=30,
        max_value=3600,
    )
    crm_rate_limit_ip_requests = _parse_int_env(
        "CRM_RATE_LIMIT_IP_REQUESTS",
        180,
        min_value=1,
        max_value=5000,
    )
    lead_radar_interval_seconds = _parse_int_env(
        "LEAD_RADAR_INTERVAL_SECONDS",
        3600,
        min_value=60,
        max_value=86400,
    )
    lead_radar_no_reply_hours = _parse_int_env(
        "LEAD_RADAR_NO_REPLY_HOURS",
        6,
        min_value=1,
        max_value=168,
    )
    lead_radar_call_no_next_step_hours = _parse_int_env(
        "LEAD_RADAR_CALL_NO_NEXT_STEP_HOURS",
        24,
        min_value=1,
        max_value=168,
    )
    lead_radar_stale_warm_days = _parse_int_env(
        "LEAD_RADAR_STALE_WARM_DAYS",
        7,
        min_value=1,
        max_value=180,
    )
    lead_radar_max_items_per_run = _parse_int_env(
        "LEAD_RADAR_MAX_ITEMS_PER_RUN",
        50,
        min_value=1,
        max_value=500,
    )
    lead_radar_thread_cooldown_hours = _parse_int_env(
        "LEAD_RADAR_THREAD_COOLDOWN_HOURS",
        24,
        min_value=0,
        max_value=240,
    )
    lead_radar_daily_cap_per_thread = _parse_int_env(
        "LEAD_RADAR_DAILY_CAP_PER_THREAD",
        2,
        min_value=1,
        max_value=20,
    )
    faq_lab_interval_seconds = _parse_int_env(
        "FAQ_LAB_INTERVAL_SECONDS",
        21600,
        min_value=300,
        max_value=7 * 24 * 3600,
    )
    faq_lab_window_days = _parse_int_env(
        "FAQ_LAB_WINDOW_DAYS",
        90,
        min_value=1,
        max_value=365,
    )
    faq_lab_min_question_count = _parse_int_env(
        "FAQ_LAB_MIN_QUESTION_COUNT",
        2,
        min_value=1,
        max_value=100,
    )
    faq_lab_max_items_per_run = _parse_int_env(
        "FAQ_LAB_MAX_ITEMS_PER_RUN",
        120,
        min_value=1,
        max_value=1000,
    )
    mango_poll_interval_seconds = _parse_int_env(
        "MANGO_POLL_INTERVAL_SECONDS",
        300,
        min_value=30,
        max_value=3600,
    )
    mango_call_recording_ttl_hours = _parse_int_env(
        "MANGO_CALL_RECORDING_TTL_HOURS",
        48,
        min_value=1,
        max_value=24 * 90,
    )
    mango_poll_limit_per_run = _parse_int_env(
        "MANGO_POLL_LIMIT_PER_RUN",
        50,
        min_value=1,
        max_value=500,
    )
    mango_poll_retry_attempts = _parse_int_env(
        "MANGO_POLL_RETRY_ATTEMPTS",
        3,
        min_value=1,
        max_value=10,
    )
    mango_poll_retry_backoff_seconds = _parse_int_env(
        "MANGO_POLL_RETRY_BACKOFF_SECONDS",
        2,
        min_value=0,
        max_value=60,
    )
    mango_retry_failed_limit_per_run = _parse_int_env(
        "MANGO_RETRY_FAILED_LIMIT_PER_RUN",
        25,
        min_value=1,
        max_value=500,
    )
    mango_webhook_path = os.getenv("MANGO_WEBHOOK_PATH", "/integrations/mango/webhook").strip()
    if not mango_webhook_path:
        mango_webhook_path = "/integrations/mango/webhook"
    if not mango_webhook_path.startswith("/"):
        mango_webhook_path = f"/{mango_webhook_path}"
    mango_calls_path = os.getenv("MANGO_CALLS_PATH", "/calls").strip()
    if not mango_calls_path:
        mango_calls_path = "/calls"
    if not mango_calls_path.startswith("/"):
        mango_calls_path = f"/{mango_calls_path}"
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
        app_env=app_env,
        startup_preflight_mode=startup_preflight_mode,
        running_on_render=running_on_render,
        persistent_data_root=persistent_data_root,
        rate_limit_backend=rate_limit_backend,
        redis_url=redis_url,
        admin_ui_csrf_enabled=admin_ui_csrf_enabled,
        assistant_api_token=os.getenv("ASSISTANT_API_TOKEN", "").strip(),
        assistant_rate_limit_window_seconds=assistant_rate_limit_window_seconds,
        assistant_rate_limit_user_requests=assistant_rate_limit_user_requests,
        assistant_rate_limit_ip_requests=assistant_rate_limit_ip_requests,
        crm_api_exposed=crm_api_exposed,
        crm_rate_limit_window_seconds=crm_rate_limit_window_seconds,
        crm_rate_limit_ip_requests=crm_rate_limit_ip_requests,
        enable_business_inbox=_parse_bool_env("ENABLE_BUSINESS_INBOX", default=False),
        enable_call_copilot=_parse_bool_env("ENABLE_CALL_COPILOT", default=False),
        enable_tallanto_enrichment=_parse_bool_env("ENABLE_TALLANTO_ENRICHMENT", default=False),
        enable_director_agent=_parse_bool_env("ENABLE_DIRECTOR_AGENT", default=False),
        enable_lead_radar=_parse_bool_env("ENABLE_LEAD_RADAR", default=False),
        enable_faq_lab=_parse_bool_env("ENABLE_FAQ_LAB", default=False),
        enable_mango_auto_ingest=_parse_bool_env("ENABLE_MANGO_AUTO_INGEST", default=False),
        lead_radar_scheduler_enabled=_parse_bool_env("LEAD_RADAR_SCHEDULER_ENABLED", default=True),
        lead_radar_interval_seconds=lead_radar_interval_seconds,
        lead_radar_no_reply_hours=lead_radar_no_reply_hours,
        lead_radar_call_no_next_step_hours=lead_radar_call_no_next_step_hours,
        lead_radar_stale_warm_days=lead_radar_stale_warm_days,
        lead_radar_max_items_per_run=lead_radar_max_items_per_run,
        lead_radar_thread_cooldown_hours=lead_radar_thread_cooldown_hours,
        lead_radar_daily_cap_per_thread=lead_radar_daily_cap_per_thread,
        faq_lab_scheduler_enabled=_parse_bool_env("FAQ_LAB_SCHEDULER_ENABLED", default=True),
        faq_lab_interval_seconds=faq_lab_interval_seconds,
        faq_lab_window_days=faq_lab_window_days,
        faq_lab_min_question_count=faq_lab_min_question_count,
        faq_lab_max_items_per_run=faq_lab_max_items_per_run,
        mango_api_base_url=os.getenv("MANGO_API_BASE_URL", "").strip(),
        mango_api_token=os.getenv("MANGO_API_TOKEN", "").strip(),
        mango_calls_path=mango_calls_path,
        mango_webhook_path=mango_webhook_path,
        mango_webhook_secret=os.getenv("MANGO_WEBHOOK_SECRET", "").strip(),
        mango_polling_enabled=_parse_bool_env("MANGO_POLLING_ENABLED", default=False),
        mango_poll_interval_seconds=mango_poll_interval_seconds,
        mango_call_recording_ttl_hours=mango_call_recording_ttl_hours,
        mango_poll_limit_per_run=mango_poll_limit_per_run,
        mango_poll_retry_attempts=mango_poll_retry_attempts,
        mango_poll_retry_backoff_seconds=mango_poll_retry_backoff_seconds,
        mango_retry_failed_limit_per_run=mango_retry_failed_limit_per_run,
    )
