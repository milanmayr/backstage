#!/usr/bin/env python3
"""
Clean up orphaned Backstage entities.

Mirrors contrib/scripts/orphan-clean-up/orphan_cleanup.sh but uses Python
with basic error handling. Defaults to http://localhost:7007 when no base
URL is provided.
"""

import argparse
import json
import sys
import urllib.error
import urllib.request
from typing import Any, Dict, List


def build_headers(api_key: str | None = None) -> Dict[str, str]:
    headers: Dict[str, str] = {"Accept": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def fetch_orphans(base_url: str, headers: Dict[str, str]) -> List[Any]:
    url = f"{base_url}/api/catalog/entities?filter=metadata.annotations.backstage.io/orphan=true"
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req) as resp:
            payload = resp.read()
    except urllib.error.HTTPError as err:
        raise RuntimeError(f"Failed to fetch orphans ({err.code}): {err.reason}") from err
    except urllib.error.URLError as err:
        raise RuntimeError(f"Failed to reach {url}: {err.reason}") from err

    try:
        data = json.loads(payload)
    except json.JSONDecodeError as err:
        raise RuntimeError(f"Invalid JSON from {url}: {err}") from err

    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected response format (expected list, got {type(data).__name__})")
    return data


def delete_orphan(base_url: str, uid: str, headers: Dict[str, str]) -> None:
    url = f"{base_url}/api/catalog/entities/by-uid/{uid}"
    req = urllib.request.Request(url, headers=headers, method="DELETE")
    try:
        with urllib.request.urlopen(req) as resp:
            if resp.status and resp.status >= 300:
                raise RuntimeError(f"Delete failed ({resp.status}) for uid {uid}")
    except urllib.error.HTTPError as err:
        raise RuntimeError(f"Delete failed for uid {uid} ({err.code}): {err.reason}") from err
    except urllib.error.URLError as err:
        raise RuntimeError(f"Failed to reach {url}: {err.reason}") from err


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description="Delete orphaned Backstage catalog entities.")
    parser.add_argument(
        "base_url",
        nargs="?",
        default="http://localhost:7007",
        help="Backstage base URL (default: http://localhost:7007)",
    )
    parser.add_argument(
        "--api-key",
        dest="api_key",
        default=None,
        help="Optional API key or token passed as Bearer to Backstage requests",
    )
    parser.add_argument(
        "--dry-run",
        "-n",
        action="store_true",
        help="Show orphan entities without deleting them",
    )
    args = parser.parse_args(argv)

    base_url = args.base_url.rstrip("/")
    print(base_url)

    headers = build_headers(args.api_key)

    try:
        orphans = fetch_orphans(base_url, headers)
    except RuntimeError as err:
        print(err, file=sys.stderr)
        return 1

    print("")
    print(f"Found {len(orphans)} orphaned entities")
    print("")

    if not orphans:
        return 0

    for orphan in orphans:
        print(json.dumps(orphan, indent=2))
        name = orphan.get("metadata", {}).get("name", "<unknown>")
        kind = orphan.get("kind", "<unknown>")
        uid = orphan.get("metadata", {}).get("uid")
        if not uid:
            print(f"Skipping entity {name} of kind {kind}: missing uid", file=sys.stderr)
            continue

        if args.dry_run:
            print(f"Dry-run: would delete orphan entity: {name} of kind: {kind}")
            continue

        print(f"Deleting orphan entity: {name} of kind: {kind}")
        try:
            delete_orphan(base_url, uid, headers)
        except RuntimeError as err:
            print(err, file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

