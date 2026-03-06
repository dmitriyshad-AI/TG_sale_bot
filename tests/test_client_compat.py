from __future__ import annotations

import asyncio
from typing import Any

import httpx


class CompatAsyncAsgiClient:
    def __init__(self, app: Any, *, base_url: str = "http://testserver", follow_redirects: bool = True) -> None:
        self._app = app
        self._base_url = base_url
        self._follow_redirects = follow_redirects

    async def _request_async(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        async def _call_once() -> httpx.Response:
            transport = httpx.ASGITransport(app=self._app)
            async with httpx.AsyncClient(
                transport=transport,
                base_url=self._base_url,
                follow_redirects=self._follow_redirects,
            ) as client:
                response = await client.request(method, url, **kwargs)
                await asyncio.sleep(0)
                return response

        router = getattr(self._app, "router", None)
        lifespan_context = getattr(router, "lifespan_context", None)
        if callable(lifespan_context):
            async with lifespan_context(self._app):
                return await _call_once()
        return await _call_once()

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        return asyncio.run(self._request_async(method, url, **kwargs))

    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("POST", url, **kwargs)

    def put(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("PUT", url, **kwargs)

    def patch(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("PATCH", url, **kwargs)

    def delete(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("DELETE", url, **kwargs)

    def close(self) -> None:
        return None

    def __enter__(self) -> "CompatAsyncAsgiClient":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()
        return None


def build_test_client(app: Any) -> Any:
    from fastapi.testclient import TestClient

    try:
        return TestClient(app)
    except TypeError:
        return CompatAsyncAsgiClient(app)
