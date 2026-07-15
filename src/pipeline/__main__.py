"""CLI: python -m pipeline run --city berlin --mode full|delta [--source ra] [--limit N] [--no-llm]"""

from __future__ import annotations

import argparse
import json
import logging
import sys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pipeline")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="full scrape → store → dedup → categorise → export")
    p_run.add_argument("--city", default="berlin")
    p_run.add_argument("--mode", choices=["full", "delta"], default="full")
    p_run.add_argument("--source", action="append", help="restrict to source slug(s)")
    p_run.add_argument("--limit", type=int, help="max events per source (smoke tests)")
    p_run.add_argument("--no-llm", action="store_true")
    p_run.add_argument("--llm-poll", type=int, default=1200, help="seconds to wait for the batch")

    p_export = sub.add_parser("export", help="re-export static JSON from the DB only")
    p_export.add_argument("--city", default="berlin")

    p_venues = sub.add_parser("venues-bootstrap", help="pull venue POIs from Overpass/OSM")
    p_venues.add_argument("--city", default="berlin")

    p_sync = sub.add_parser("sync-d1", help="push the export window into Gobento's Cloudflare D1")
    p_sync.add_argument("--city", default="berlin")
    p_sync.add_argument("--no-prune", action="store_true",
                        help="upsert only; don't delete D1 rows missing from this export "
                             "(use for limited/partial scrapes)")

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )

    if args.cmd == "run":
        from .runner import run

        telemetry = run(
            args.city, mode=args.mode, only=args.source, limit=args.limit,
            no_llm=args.no_llm, llm_poll=args.llm_poll,
        )
        print(json.dumps(telemetry, indent=2))
        return 0

    if args.cmd == "export":
        from . import config as cfg
        from .db import connect
        from .export import export_city

        conn = connect(cfg.DB_PATH)
        print(json.dumps(export_city(conn, args.city, cfg.PUBLIC_DIR)))
        return 0

    if args.cmd == "sync-d1":
        from .sync_d1 import sync

        print(json.dumps(sync(args.city, prune=not args.no_prune)))
        return 0

    if args.cmd == "venues-bootstrap":
        from . import config as cfg
        from .db import connect
        from .fetch import Fetcher
        from .venues import bootstrap_from_overpass

        conn = connect(cfg.DB_PATH)
        fetcher = Fetcher(cache_path=None)
        added = bootstrap_from_overpass(conn, fetcher)
        conn.commit()
        print(f"added {added} venues from OSM (ODbL — attribute if redistributed)")
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
