#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import mimetypes
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sales_agent.sales_core.config import get_settings
from sales_agent.sales_core.vector_store import load_vector_store_id, write_vector_store_meta


API_BASE = "https://api.openai.com/v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create/update OpenAI vector store from knowledge files")
    parser.add_argument(
        "--knowledge-dir",
        type=Path,
        default=None,
        help="Directory with knowledge files (default: from settings)",
    )
    parser.add_argument(
        "--meta-path",
        type=Path,
        default=None,
        help="Where to store vector_store_id metadata (default: from settings)",
    )
    parser.add_argument(
        "--vector-store-id",
        type=str,
        default=None,
        help="Use existing vector store id (default: OPENAI_VECTOR_STORE_ID or data/vector_store.json)",
    )
    parser.add_argument(
        "--name",
        type=str,
        default="sales-agent-knowledge",
        help="Name for a new vector store",
    )
    return parser.parse_args()


def _request_json(url: str, api_key: str, payload: Dict, timeout: float = 30.0) -> Dict:
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _upload_file(api_key: str, file_path: Path) -> str:
    boundary = f"----sales-agent-{uuid.uuid4().hex}"
    mime_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
    file_data = file_path.read_bytes()

    parts: List[bytes] = []
    parts.append(f"--{boundary}\r\n".encode("utf-8"))
    parts.append(b'Content-Disposition: form-data; name="purpose"\r\n\r\n')
    parts.append(b"assistants\r\n")
    parts.append(f"--{boundary}\r\n".encode("utf-8"))
    parts.append(
        (
            f'Content-Disposition: form-data; name="file"; filename="{file_path.name}"\r\n'
            f"Content-Type: {mime_type}\r\n\r\n"
        ).encode("utf-8")
    )
    parts.append(file_data)
    parts.append(b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode("utf-8"))
    body = b"".join(parts)

    request = Request(
        f"{API_BASE}/files",
        data=body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        },
        method="POST",
    )
    with urlopen(request, timeout=60.0) as response:
        data = json.loads(response.read().decode("utf-8"))
    file_id = data.get("id")
    if not isinstance(file_id, str) or not file_id:
        raise RuntimeError(f"File upload returned invalid id for {file_path.name}: {data}")
    return file_id


def _attach_file_to_vector_store(api_key: str, vector_store_id: str, file_id: str) -> None:
    payload = {"file_id": file_id}
    _request_json(f"{API_BASE}/vector_stores/{vector_store_id}/files", api_key, payload)


def _create_vector_store(api_key: str, name: str) -> str:
    payload = {"name": name}
    response = _request_json(f"{API_BASE}/vector_stores", api_key, payload)
    vector_store_id = response.get("id")
    if not isinstance(vector_store_id, str) or not vector_store_id:
        raise RuntimeError(f"Vector store creation failed: {response}")
    return vector_store_id


def main() -> int:
    args = parse_args()
    settings = get_settings()

    api_key = settings.openai_api_key
    if not api_key:
        print("[ERROR] OPENAI_API_KEY is not set")
        return 1

    knowledge_dir = args.knowledge_dir or settings.knowledge_path
    meta_path = args.meta_path or settings.vector_store_meta_path

    if not knowledge_dir.exists() or not knowledge_dir.is_dir():
        print(f"[ERROR] knowledge directory not found: {knowledge_dir}")
        return 1

    knowledge_files = sorted(
        [
            path
            for path in knowledge_dir.iterdir()
            if path.is_file() and path.suffix.lower() in {".md", ".txt", ".pdf", ".docx"}
        ]
    )
    if not knowledge_files:
        print(f"[ERROR] no knowledge files found in {knowledge_dir}")
        return 1

    vector_store_id = (
        args.vector_store_id
        or settings.openai_vector_store_id
        or load_vector_store_id(meta_path)
    )

    try:
        if not vector_store_id:
            vector_store_id = _create_vector_store(api_key=api_key, name=args.name)
            print(f"[INFO] created vector store: {vector_store_id}")
        else:
            print(f"[INFO] using vector store: {vector_store_id}")

        uploaded: List[Dict[str, str]] = []
        for file_path in knowledge_files:
            file_id = _upload_file(api_key=api_key, file_path=file_path)
            _attach_file_to_vector_store(
                api_key=api_key,
                vector_store_id=vector_store_id,
                file_id=file_id,
            )
            uploaded.append({"name": file_path.name, "file_id": file_id})
            print(f"[OK] uploaded {file_path.name} -> {file_id}")

        write_vector_store_meta(
            path=meta_path,
            payload={
                "vector_store_id": vector_store_id,
                "knowledge_dir": str(knowledge_dir),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "files": uploaded,
            },
        )
        print(f"[OK] metadata saved to {meta_path}")
        return 0

    except HTTPError as exc:
        print(f"[ERROR] OpenAI HTTP error: {exc.code}")
        return 1
    except URLError as exc:
        print(f"[ERROR] OpenAI connection error: {exc.reason}")
        return 1
    except Exception as exc:
        print(f"[ERROR] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
