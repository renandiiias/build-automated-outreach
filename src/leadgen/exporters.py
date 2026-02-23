from __future__ import annotations

from pathlib import Path

import pandas as pd


def export_csv(rows: list[dict], out_file: Path) -> Path:
    df = pd.DataFrame(rows)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_file, index=False)
    return out_file


def export_xlsx(rows: list[dict], out_file: Path) -> Path:
    df = pd.DataFrame(rows)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(out_file, index=False)
    return out_file
