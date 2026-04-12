from __future__ import annotations

import argparse
import asyncio
import logging

import httpx

from app.core.config import settings
from app.main import app


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke-test future-buildability rollout guardrails")
    parser.add_argument(
        "--expected-state",
        choices=("enabled", "disabled"),
        default="enabled",
        help="Expected feature-flag state in the current environment",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1,
        help="Limit used for smoke requests",
    )
    return parser.parse_args(argv)


async def _main() -> None:
    args = parse_args()
    expected_enabled = args.expected_state == "enabled"
    if settings.future_buildability_enabled != expected_enabled:
        raise SystemExit(
            "future_buildability feature flag mismatch: "
            f"expected={args.expected_state} actual={'enabled' if settings.future_buildability_enabled else 'disabled'}"
        )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        endpoints = {
            "/api/v1/leads?strategy_type=current_buildable&limit=%d" % args.limit: 200,
            "/api/v1/leads?strategy_type=future_buildable&limit=%d" % args.limit: (
                200 if expected_enabled else 503
            ),
            "/api/v1/future_buildability_signals?limit=%d" % args.limit: (
                200 if expected_enabled else 503
            ),
        }
        failures: list[str] = []
        for endpoint, expected_status in endpoints.items():
            response = await client.get(endpoint)
            if response.status_code != expected_status:
                failures.append(
                    f"{endpoint}: expected {expected_status}, got {response.status_code}"
                )
        if failures:
            raise SystemExit("future_buildability_smoke failed:\n  - " + "\n  - ".join(failures))

    print(
        "future_buildability_smoke "
        f"feature={'enabled' if expected_enabled else 'disabled'} "
        f"limit={args.limit} ok"
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    asyncio.run(_main())
