from fastapi import FastAPI

from sales_agent.sales_core.config import get_settings
from sales_agent.sales_core.db import init_db

settings = get_settings()
init_db(settings.database_path)

app = FastAPI(title="sales-agent")


@app.get("/api/health")
async def health():
    return {"status": "ok", "service": "sales-agent"}

