from __future__ import annotations

import argparse
import asyncio
import logging

from app.services.planning_signal_sync import probe_html_index_registry, run_planning_signal_sync


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync normalized planning signals")
    parser.add_argument("--teryt", dest="teryt_gmina", help="Optional 7-char gmina TERYT")
    parser.add_argument(
        "--probe-html-index",
        action="store_true",
        help="Only probe html_index/planning pages and print source health",
    )
    return parser.parse_args()


async def _main() -> None:
    args = parse_args()
    if args.probe_html_index:
        results = await probe_html_index_registry(teryt_gmina=args.teryt_gmina)
        for result in results:
            print(
                "planning_signal_probe "
                f"teryt={result.teryt_gmina} "
                f"status={result.status} "
                f"signals={result.signals_detected} "
                f"url={result.source_url}"
                + (f" error={result.error}" if result.error else "")
            )
        return
    report = await run_planning_signal_sync(teryt_gmina=args.teryt_gmina)
    print(
        "planning_signal_sync "
        f"scanned={report.scanned_zones} "
        f"created={report.signals_created} "
        f"updated={report.signals_updated} "
        f"duration_s={report.duration_s}"
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    asyncio.run(_main())
