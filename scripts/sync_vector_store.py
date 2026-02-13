#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sales_agent.sales_core.config import get_settings
from sales_agent.sales_core.vector_store import (
    load_vector_store_id,
    read_vector_store_meta,
    write_vector_store_meta,
)


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
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned actions without uploading, deleting, or writing metadata",
    )
    parser.add_argument(
        "--prune-missing",
        action="store_true",
        help="Delete files from vector store if they are no longer present in knowledge directory",
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


def _request_json_get(url: str, api_key: str, timeout: float = 30.0) -> Dict:
    request = Request(
        url,
        headers={"Authorization": f"Bearer {api_key}"},
        method="GET",
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


def _delete_file_from_vector_store(api_key: str, vector_store_id: str, file_id: str, timeout: float = 30.0) -> None:
    request = Request(
        f"{API_BASE}/vector_stores/{vector_store_id}/files/{file_id}",
        headers={"Authorization": f"Bearer {api_key}"},
        method="DELETE",
    )
    with urlopen(request, timeout=timeout):
        return None


def _list_vector_store_files(
    api_key: str,
    vector_store_id: str,
    *,
    timeout: float = 30.0,
    page_size: int = 100,
) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    after: Optional[str] = None
    while True:
        url = f"{API_BASE}/vector_stores/{vector_store_id}/files?limit={page_size}"
        if after:
            url = f"{url}&after={after}"
        payload = _request_json_get(url, api_key, timeout=timeout)
        data = payload.get("data", [])
        if not isinstance(data, list):
            break

        for entry in data:
            if not isinstance(entry, dict):
                continue
            file_id = entry.get("id") or entry.get("file_id")
            if not isinstance(file_id, str) or not file_id.strip():
                continue
            name = entry.get("filename")
            if not isinstance(name, str):
                name = ""
            items.append({"file_id": file_id.strip(), "name": name.strip()})

        has_more = bool(payload.get("has_more"))
        if not has_more:
            break
        last_id = payload.get("last_id")
        if not isinstance(last_id, str) or not last_id.strip():
            break
        after = last_id.strip()

    return items


def _file_sha256(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as fh:
        while True:
            chunk = fh.read(8192)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _index_existing_files(meta: Dict[str, Any], vector_store_id: str) -> Dict[str, Dict[str, str]]:
    if meta.get("vector_store_id") != vector_store_id:
        return {}

    raw_files = meta.get("files")
    if not isinstance(raw_files, list):
        return {}

    indexed: Dict[str, Dict[str, str]] = {}
    for item in raw_files:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        file_id = item.get("file_id")
        sha256 = item.get("sha256")
        if not isinstance(name, str) or not name.strip():
            continue
        if not isinstance(file_id, str) or not file_id.strip():
            continue
        if not isinstance(sha256, str) or not sha256.strip():
            continue
        indexed[name] = {
            "file_id": file_id.strip(),
            "sha256": sha256.strip(),
        }
    return indexed


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

    existing_meta = read_vector_store_meta(meta_path)

    vector_store_id = (
        args.vector_store_id
        or settings.openai_vector_store_id
        or load_vector_store_id(meta_path)
    )

    try:
        if not vector_store_id:
            if args.dry_run:
                vector_store_id = "dry_run_new_vector_store"
                print(f"[DRY-RUN] would create vector store with name: {args.name}")
            else:
                vector_store_id = _create_vector_store(api_key=api_key, name=args.name)
                print(f"[INFO] created vector store: {vector_store_id}")
        else:
            print(f"[INFO] using vector store: {vector_store_id}")

        existing_file_map = _index_existing_files(existing_meta, vector_store_id=vector_store_id)
        uploaded_files = 0
        skipped_files = 0
        removed_files = 0
        synced_files: List[Dict[str, object]] = []
        replaced_file_refs: List[Dict[str, str]] = []
        current_names = {path.name for path in knowledge_files}

        for file_path in knowledge_files:
            sha256 = _file_sha256(file_path)
            previous = existing_file_map.get(file_path.name)
            if previous and previous.get("sha256") == sha256:
                file_id = str(previous["file_id"])
                skipped_files += 1
                status = "reused"
                print(f"[SKIP] unchanged {file_path.name} -> {file_id}")
            else:
                if args.dry_run:
                    file_id = str(previous["file_id"]) if previous else "<new-file-id>"
                    status = "would_upload"
                    uploaded_files += 1
                    print(f"[DRY-RUN] would upload {file_path.name}")
                else:
                    file_id = _upload_file(api_key=api_key, file_path=file_path)
                    _attach_file_to_vector_store(
                        api_key=api_key,
                        vector_store_id=vector_store_id,
                        file_id=file_id,
                    )
                    uploaded_files += 1
                    status = "uploaded"
                    print(f"[OK] uploaded {file_path.name} -> {file_id}")
                if previous and previous.get("file_id") and previous.get("file_id") != file_id:
                    replaced_file_refs.append(
                        {
                            "name": file_path.name,
                            "file_id": str(previous["file_id"]),
                            "sha256": str(previous.get("sha256") or ""),
                            "reason": "replaced",
                        }
                    )

            synced_files.append(
                {
                    "name": file_path.name,
                    "file_id": file_id,
                    "sha256": sha256,
                    "size_bytes": file_path.stat().st_size,
                    "status": status,
                }
            )

        stale_file_candidates: Dict[str, Dict[str, str]] = {}
        for stale_name in (name for name in existing_file_map if name not in current_names):
            stale_file = existing_file_map[stale_name]
            stale_file_id = stale_file["file_id"]
            stale_file_candidates[stale_file_id] = {
                "name": stale_name,
                "file_id": stale_file_id,
                "sha256": stale_file["sha256"],
                "reason": "missing_local_file",
            }

        for replaced in replaced_file_refs:
            stale_file_candidates[replaced["file_id"]] = replaced

        if args.prune_missing:
            try:
                remote_files = _list_vector_store_files(api_key=api_key, vector_store_id=vector_store_id)
                known_ids = {
                    str(item.get("file_id"))
                    for item in synced_files
                    if isinstance(item.get("file_id"), str) and item.get("file_id")
                }
                for remote in remote_files:
                    remote_id = remote["file_id"]
                    if remote_id in known_ids:
                        continue
                    if remote_id in stale_file_candidates:
                        continue
                    stale_file_candidates[remote_id] = {
                        "name": remote.get("name") or "<unknown>",
                        "file_id": remote_id,
                        "sha256": "",
                        "reason": "not_in_synced_set",
                    }
            except Exception as exc:
                print(f"[WARN] failed to list remote vector store files: {exc}")

        for stale_file in stale_file_candidates.values():
            stale_name = stale_file["name"]
            stale_file_id = stale_file["file_id"]
            stale_reason = stale_file.get("reason") or "stale"
            if not args.prune_missing:
                synced_files.append(
                    {
                        "name": stale_name,
                        "file_id": stale_file_id,
                        "sha256": stale_file["sha256"],
                        "status": "orphaned",
                        "reason": stale_reason,
                    }
                )
                print(
                    f"[WARN] stale file left in vector store: {stale_name} -> "
                    f"{stale_file_id} ({stale_reason})"
                )
                continue

            if args.dry_run:
                removed_files += 1
                print(
                    f"[DRY-RUN] would remove stale file from vector store: {stale_name} -> "
                    f"{stale_file_id} ({stale_reason})"
                )
                continue

            _delete_file_from_vector_store(
                api_key=api_key,
                vector_store_id=vector_store_id,
                file_id=stale_file_id,
            )
            removed_files += 1
            print(
                f"[OK] removed stale file from vector store: {stale_name} -> "
                f"{stale_file_id} ({stale_reason})"
            )

        if args.dry_run:
            print(
                f"[INFO] dry-run complete: upload={uploaded_files}, reused={skipped_files}, "
                f"remove={removed_files}, total={len(synced_files)}"
            )
            print("[INFO] metadata is not written in dry-run mode")
            return 0

        write_vector_store_meta(
            path=meta_path,
            payload={
                "vector_store_id": vector_store_id,
                "knowledge_dir": str(knowledge_dir),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "files": synced_files,
                "stats": {
                    "uploaded": uploaded_files,
                    "reused": skipped_files,
                    "removed": removed_files,
                    "total": len(synced_files),
                },
            },
        )
        print(
            f"[INFO] sync complete: uploaded={uploaded_files}, reused={skipped_files}, "
            f"removed={removed_files}, total={len(synced_files)}"
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
