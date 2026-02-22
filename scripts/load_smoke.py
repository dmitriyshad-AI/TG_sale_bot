#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, Optional
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen


@dataclass
class ProbeResult:
    status_code: int
    latency_ms: float
    error: str = ""


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Lightweight load smoke for API endpoints.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="API base URL.")
    parser.add_argument(
        "--target",
        choices=("assistant", "catalog", "health"),
        default="assistant",
        help="Endpoint profile to test.",
    )
    parser.add_argument("--requests", type=int, default=40, help="Total requests count.")
    parser.add_argument("--concurrency", type=int, default=8, help="Concurrent workers.")
    parser.add_argument("--assistant-token", default="", help="Assistant API token for /api/assistant/ask.")
    parser.add_argument("--timeout", type=float, default=12.0, help="Single request timeout in seconds.")
    return parser


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    sorted_values = sorted(values)
    rank = max(0.0, min(1.0, percentile)) * (len(sorted_values) - 1)
    lower = int(rank)
    upper = min(lower + 1, len(sorted_values) - 1)
    weight = rank - lower
    return sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight


def _probe_once(
    *,
    base_url: str,
    target: str,
    timeout: float,
    assistant_token: str,
) -> ProbeResult:
    start = time.perf_counter()
    headers: Dict[str, str] = {}
    data: Optional[bytes] = None

    if target == "assistant":
        url = urljoin(base_url.rstrip("/") + "/", "api/assistant/ask")
        body = {
            "question": "Коротко: как начать готовиться к ЕГЭ по математике в 10 классе?",
            "criteria": {"brand": "kmipt", "grade": 10, "goal": "ege", "subject": "math", "format": "online"},
        }
        headers["Content-Type"] = "application/json"
        if assistant_token.strip():
            headers["X-Assistant-Token"] = assistant_token.strip()
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        method = "POST"
    elif target == "catalog":
        query = urlencode(
            {
                "brand": "kmipt",
                "grade": "10",
                "goal": "ege",
                "subject": "math",
                "format": "online",
            }
        )
        url = urljoin(base_url.rstrip("/") + "/", f"api/catalog/search?{query}")
        method = "GET"
    else:
        url = urljoin(base_url.rstrip("/") + "/", "api/health")
        method = "GET"

    request = Request(url, headers=headers, data=data, method=method)
    try:
        with urlopen(request, timeout=timeout) as response:
            status = int(response.status)
            response.read()
    except Exception as exc:
        latency_ms = (time.perf_counter() - start) * 1000.0
        return ProbeResult(status_code=0, latency_ms=latency_ms, error=str(exc))

    latency_ms = (time.perf_counter() - start) * 1000.0
    return ProbeResult(status_code=status, latency_ms=latency_ms)


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    total_requests = max(1, int(args.requests))
    concurrency = max(1, min(total_requests, int(args.concurrency)))

    results: list[ProbeResult] = []
    started_at = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [
            pool.submit(
                _probe_once,
                base_url=args.base_url,
                target=args.target,
                timeout=float(args.timeout),
                assistant_token=args.assistant_token,
            )
            for _ in range(total_requests)
        ]
        for future in as_completed(futures):
            results.append(future.result())

    elapsed = max(0.001, time.perf_counter() - started_at)
    latencies = [item.latency_ms for item in results]
    ok_count = sum(1 for item in results if 200 <= item.status_code < 300)
    error_count = sum(1 for item in results if item.error)
    rps = len(results) / elapsed

    print(f"target={args.target} total={len(results)} ok={ok_count} errors={error_count} rps={rps:.2f}")
    print(
        "latency_ms "
        f"min={min(latencies):.1f} "
        f"p50={_percentile(latencies, 0.50):.1f} "
        f"p95={_percentile(latencies, 0.95):.1f} "
        f"max={max(latencies):.1f} "
        f"avg={statistics.mean(latencies):.1f}"
    )

    if ok_count != len(results):
        failures = [item for item in results if not (200 <= item.status_code < 300)]
        first_failure = failures[0]
        details = first_failure.error or f"status={first_failure.status_code}"
        print(f"[FAIL] Non-2xx responses detected: {details}")
        return 1

    print("[OK] Load smoke finished without non-2xx responses.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
