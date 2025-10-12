# file: tools/fix_ticket_headers.py
from __future__ import annotations

import io
import os
import re
import zipfile
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Iterable

import pandas as pd

# --- config: sinonim -> target
REQUIRED_MAP: Dict[str, List[str]] = {
    "created_action": [
        "created", "created_date", "create_date", "createdate", "action", "action_time",
        "tanggal", "tanggal_dibuat", "tgl_buat", "waktu_transaksi", "transaction_time",
        "created/action", "created_at", "tgl_transaksi", "date", "datetime",
    ],
    "amount": [
        "amount", "tarif", "nominal", "nilai", "gross_amount", "total", "jumlah", "price",
        "transaction_amount", "grand_total",
    ],
    "status": [
        "status", "payment_status", "st_bayar", "paid_status", "status_pembayaran",
        "trx_status", "state",
    ],
    "bank_channel": [
        "bank", "channel", "payment_channel", "metode", "via", "issuer", "acquirer",
        "bank/channel", "bank_channel", "payment_method",
    ],
}

# normalisasi kolom: lower, hapus spasi/sep, ASCII only
_norm_re = re.compile(r"[^a-z0-9]+")

def normalize(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("é", "e").replace("’", "'")
    return _norm_re.sub("_", s).strip("_")

def best_header_row(df: pd.DataFrame, scan_rows: int = 5) -> int:
    """Cari baris yang paling mungkin header (0-based)."""
    best_row, best_score = 0, -1
    for r in range(min(scan_rows, len(df))):
        row_vals = [str(x) for x in df.iloc[r].tolist()]
        score = 0
        for v in row_vals:
            nv = normalize(v)
            # poin jika terlihat seperti nama kolom
            if nv and not nv.isdigit():
                score += 1
            # bonus kalau match salah satu sinonim
            if any(nv in [normalize(s) for s in syns] for syns in REQUIRED_MAP.values()):
                score += 3
        if score > best_score:
            best_row, best_score = r, score
    return best_row

def map_required(columns: Iterable[str]) -> Tuple[Dict[str, str], List[str]]:
    """Return mapping {target: source_col} dan daftar target yang hilang."""
    norm_cols = {normalize(c): c for c in columns}
    mapped: Dict[str, str] = {}
    missing: List[str] = []
    for target, syns in REQUIRED_MAP.items():
        found = None
        for s in syns:
            key = normalize(s)
            if key in norm_cols:
                found = norm_cols[key]
                break
        if found:
            mapped[target] = found
        else:
            missing.append(target)
    return mapped, missing

@dataclass
class FixResult:
    source: str
    sheet: Optional[str]
    header_row: int
    mapped: Dict[str, str]
    missing: List[str]
    out_path: Optional[str]
    rows: int

def process_dataframe(df_raw: pd.DataFrame, source: str, sheet: Optional[str]) -> FixResult:
    header_row = best_header_row(df_raw)
    df = pd.read_excel(source, sheet_name=sheet, header=header_row) if isinstance(df_raw, pd.DataFrame) else df_raw  # type: ignore

    # jika df_raw dari read_excel(None), bacalah ulang dengan header_row
    if isinstance(df_raw, pd.DataFrame):
        pass

    df = df.rename(columns={c: str(c) for c in df.columns})
    mapped, missing = map_required(df.columns)

    out_path = None
    if not missing:
        # standardize names
        rename_to_standard = {v: k for k, v in mapped.items()}
        fixed = df.rename(columns=rename_to_standard)
        # minimal kolom yang dipastikan ada
        keep = ["created_action", "amount", "status", "bank_channel"]
        # simpan CSV
        base = os.path.splitext(os.path.basename(source))[0]
        suffix = f"_{sheet}" if sheet else ""
        out_path = f"{base}{suffix}_fixed.csv"
        fixed.to_csv(out_path, index=False)
    return FixResult(
        source=source,
        sheet=sheet,
        header_row=header_row,
        mapped=mapped,
        missing=missing,
        out_path=out_path,
        rows=int(df.shape[0]),
    )

def read_any(path: str) -> List[FixResult]:
    results: List[FixResult] = []
    if path.lower().endswith(".zip"):
        with zipfile.ZipFile(path) as zf:
            for name in zf.namelist():
                if name.lower().endswith((".csv", ".xlsx", ".xls")):
                    with zf.open(name) as f:
                        data = f.read()
                        buf = io.BytesIO(data)
                        results.extend(read_any_buffer(buf, name))
    else:
        results.extend(read_any_buffer(open(path, "rb"), path))
    return results

def read_any_buffer(buf_like, source_name: str) -> List[FixResult]:
    results: List[FixResult] = []
    name = source_name.lower()
    try:
        if name.endswith(".csv"):
            df = pd.read_csv(buf_like)
            results.append(process_dataframe(df, source_name, None))
        elif name.endswith((".xlsx", ".xls")):
            xls = pd.ExcelFile(buf_like)
            for sheet in xls.sheet_names:
                df0 = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=5)
                # deteksi header baris
                hdr_row = best_header_row(df0)
                df = pd.read_excel(xls, sheet_name=sheet, header=hdr_row)
                results.append(process_dataframe(df, source_name, sheet))
        else:
            print(f"Skip {source_name}: unsupported")
    except Exception as e:
        print(f"Error reading {source_name}: {e}")
    return results

def main(paths: List[str]) -> None:
    all_res: List[FixResult] = []
    for p in paths:
        all_res.extend(read_any(p))

    print("\n=== HASIL DETEKSI ===")
    for r in all_res:
        tag = f"{os.path.basename(r.source)}" + (f" | sheet={r.sheet}" if r.sheet else "")
        print(f"- {tag} | rows={r.rows} | header_row={r.header_row}")
        if r.missing:
            print(f"  MISSING: {', '.join(r.missing)}")
        else:
            print(f"  OK -> saved: {r.out_path}")
            print(f"  mapped: {r.mapped}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python fix_ticket_headers.py <file_or_zip> [more files...]")
        sys.exit(2)
    main(sys.argv[1:])
