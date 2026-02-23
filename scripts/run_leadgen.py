#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv() -> None:
        return None

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gera planilhas de leads a partir de buscas no Google Maps"
    )
    parser.add_argument("--audience", required=True, help="Ex: dentistas")
    parser.add_argument("--location", required=True, help="Ex: Sao Paulo SP")
    parser.add_argument("--max-results", type=int, default=40, help="Maximo de resultados")
    parser.add_argument(
        "--format",
        default="both",
        choices=["csv", "xlsx", "both"],
        help="Formato da saida",
    )
    parser.add_argument(
        "--headful",
        action="store_true",
        help="Executa navegador visivel (debug)",
    )
    parser.add_argument(
        "--enrich-website",
        action="store_true",
        help="Tenta enriquecer lead pelo website (emails/telefones) usando Scrapling quando disponivel",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    args = parse_args()

    from leadgen.runner import LeadGeneratorRunner

    runner = LeadGeneratorRunner()
    files = runner.run(
        audience=args.audience,
        location=args.location,
        max_results=args.max_results,
        out_format=args.format,
        headless=not args.headful,
        enrich_website=args.enrich_website,
    )
    for file_path in files:
        print(file_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
