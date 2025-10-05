# rekon_tiket_espay_app.py
# -------------------------------------------------------------
# Rekonsiliasi Tiket Detail (Created -> Action) vs Settlement ESPAY
# - Ticket Detail: Excel/CSV/ZIP (ZIP berisi CSV/XLSX/XLS)
# - Settlement : CSV/ZIP/Excel
# - Header-detection: cari baris header di 30 baris awal (mengatasi merge/judul)
# - Ekstrak 'Created' -> 'Action' (tanggal), agregasi & rekonsiliasi per tanggal
#
# Jalankan:
#   pip install streamlit pandas openpyxl xlsxwriter
#   streamlit run rekon_tiket_espay_app.py

import io
import re
import zipfile
from typing import List, Tuple, Optional

import pandas as pd
import streamlit as st

# ========================= Utilities =========================

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df

def _find_header_row(raw_df: pd.DataFrame) -> int:
    """
    Temukan baris header dengan memindai 30 baris awal dan mencari keywords.
    Mengatasi kasus ada judul/merge di baris atas.
    """
    keywords = [
        "created", "waktu validasi", "tanggal", "tgl", "date",
        "transaksi", "paid", "posting", "settlement", "action"
    ]
    max_scan = min(30, len(raw_df))
    for i in range(max_scan):
        row = raw_df.iloc[i].astype(str).str.strip().str.lower().tolist()
        if any(any(kw in cell for kw in keywords) for cell in row):
            return i
    return 0  # fallback

def _read_excel_bytes_smart(data: bytes) -> List[pd.DataFrame]:
    """Baca Excel dengan deteksi baris header (anti-merge/judul)."""
    buf = io.BytesIO(data)
    xls = pd.ExcelFile(buf)
    dfs = []
    for sh in xls.sheet_names:
        raw = pd.read_excel(xls, sheet_name=sh, header=None, dtype=object)
        hdr = _find_header_row(raw)
        df  = pd.read_excel(io.BytesIO(data), sheet_name=sh, header=hdr, dtype=object)
        df = _normalize_columns(df)
        dfs.append(df)
    return dfs

def _read_csv_bytes(data: bytes) -> pd.DataFrame:
    buf = io.BytesIO(data)
    try:
        return pd.read_csv(buf, dtype=object)
    except Exception:
        buf.seek(0)
        return pd.read_csv(buf, dtype=object, sep=";")

def _find_created_col(df: pd.DataFrame) -> Optional[str]:
    cols = [str(c).strip() for c in df.columns]
    for c in cols:
        if c.lower() == "created":
            return c
    for c in cols:
        if "created" in c.lower():
            return c
    for c in cols:
        if re.search(r"(dibuat|tgl.*buat|tanggal.*buat)", c.lower()):
            return c
    return None

def normalize_created_action(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """
    Pastikan ada:
      - 'Created' : 'dd/mm/YYYY HH:MM:SS'
      - 'Action'  : 'dd/mm/YYYY'
    """
    df = _normalize_columns(df.copy())
    created_col = _find_created_col(df)
    if created_col is None:
        return df, "Kolom 'Created' tidak ditemukan."

    s = df[created_col]
    if pd.api.types.is_datetime64_any_dtype(s):
        created_dt = pd.to_datetime(s, errors="coerce")
        df["Created"] = created_dt.dt.strftime("%d/%m/%Y %H:%M:%S")
        df["Action"]  = created_dt.dt.strftime("%d/%m/%Y")
    else:
        s = s.astype(str).str.strip()
        s = s.str.replace("T", " ", regex=False) \
             .str.replace(r"([+-]\d{2}:?\d{2}|Z)$", "", regex=True)
        d = s.str.slice(0, 10)
        t = s.str.slice(11, 19)
        d1 = pd.to_datetime(d, errors="coerce", dayfirst=True)
        d2 = pd.to_datetime(d, errors="coerce", format="%Y-%m-%d")
        dp = d1.fillna(d2)
        df["Created"] = dp.dt.strftime("%d/%m/%Y").fillna(d) + " " + t.fillna("00:00:00")
        df["Action"]  = dp.dt.strftime("%d/%m/%Y")

    # bersihkan sisa perantara
    for col in list(df.columns):
        if str(col).lower() in ["created_date_str", "created_time_str"]:
            del df[col]
    for col in list(df.columns):
        if str(col).lower() == "action" and col != "Action":
            del df[col]
    return df, "OK"

def ensure_action_column(df: pd.DataFrame) -> pd.DataFrame:
    """Anti-KeyError: pastikan 'Action' ada (turunkan dari Created / tanggal lain)."""
    if "Action" in df.columns and not df["Action"].isna().all():
        return df

    # 1) Dari Created
    cand = [c for c in df.columns if str(c).strip().lower() == "created" or "created" in str(c).strip().lower()]
    if cand:
        col = cand[0]
        s = df[col]
        if pd.api.types.is_datetime64_any_dtype(s):
            df["Action"] = pd.to_datetime(s, errors="coerce").dt.strftime("%d/%m/%Y")
            return df
        s = s.astype(str).str.strip()
        s = s.str.replace("T", " ", regex=False).str.replace(r"([+-]\d{2}:?\d{2}|Z)$", "", regex=True)
        d  = s.str.slice(0, 10)
        d1 = pd.to_datetime(d, errors="coerce", dayfirst=True)
        d2 = pd.to_datetime(d, errors="coerce", format="%Y-%m-%d")
        df["Action"] = d1.fillna(d2).dt.strftime("%d/%m/%Y")
        return df

    # 2) Dari kolom tanggal lain
    for c in df.columns:
        cl = str(c).strip().lower()
        if any(k in cl for k in ["tanggal","date","transaksi","paid","posting","settlement"]):
            dparsed = pd.to_datetime(df[c], dayfirst=True, errors="coerce")
            if dparsed.notna().any():
                df["Action"] = dparsed.dt.strftime("%d/%m/%Y")
                return df

    # 3) Fallback
    df["Action"] = pd.NaT
    return df

def guess_numeric_cols(df: pd.DataFrame) -> List[str]:
    preferred = {"amount","nominal","nilai","total","harga","tarif","jumlah","grand total","bayar","payment","gross","net"}
    cols = []
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            cols.append(str(c))
        else:
            smp = df[c].dropna().astype(str).head(20)
            if not smp.empty and smp.str.replace(r"[0-9\.,\-]", "", regex=True).str.len().max() == 0:
                cols.append(str(c))
    return sorted(cols, key=lambda x: (0 if x.strip().lower() in preferred else 1, x))

def guess_date_cols(df: pd.DataFrame) -> List[str]:
    cands, seen = [], set()
    for c in df.columns:
        cl = str(c).strip().lower()
        if any(k in cl for k in ["settlement","posting","tanggal","date","transaksi","paid","created"]) \
           or pd.api.types.is_datetime64_any_dtype(df[c]):
            if c not in seen:
                cands.append(c); seen.add(c)
    return cands

def parse_currency_series(s: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce")
    x = s.astype(str).str.strip().str.replace(r"\s", "", regex=True)
    def _one(v: str):
        if v in ("", "nan", "None", "NaN"):
            return None
        has_dot, has_com = "." in v, "," in v
        if has_dot and has_com:
            last_sep = max(v.rfind("."), v.rfind(","))
            int_d = re.sub(r"[^\d]", "", v[:last_sep])
            frac_d = re.sub(r"[^\d]", "", v[last_sep+1:])
            num = int_d + ("." + frac_d if frac_d else "")
            try: return float(num)
            except: return None
        digits = re.sub(r"[^\d\-]", "", v)
        if digits in ("", "-", "--"): return None
        try: return float(digits)
        except: return None
    return x.map(_one)

# ========================= Readers (CSV/Excel/ZIP) =========================

def read_ticket_files(files) -> pd.DataFrame:
    """Ticket: XLSX/XLS/CSV/ZIP (ZIP: CSV/XLSX/XLS) + header detection."""
    frames = []
    for f in files:
        name = getattr(f, "name", "uploaded")
        data = f.read()

        # ZIP
        if name.lower().endswith(".zip"):
            zbuf = io.BytesIO(data)
            with zipfile.ZipFile(zbuf) as z:
                for zi in z.infolist():
                    zname = zi.filename
                    with z.open(zi) as fh:
                        content = fh.read()
                        if zname.lower().endswith((".xlsx",".xls")):
                            try:
                                dfs = _read_excel_bytes_smart(content)
                            except Exception:
                                dfs = [_read_csv_bytes(content)]
                        elif zname.lower().endswith(".csv"):
                            dfs = [_read_csv_bytes(content)]
                        else:
                            continue
                        for df in dfs:
                            df2, _ = normalize_created_action(df)
                            df2["source_file"] = f"{name}:{zname}"
                            frames.append(df2)
            continue

        # Non-ZIP
        try:
            dfs = _read_excel_bytes_smart(data)
        except Exception:
            dfs = [_read_csv_bytes(data)]
        for i, df in enumerate(dfs):
            df2, _ = normalize_created_action(df)
            df2["source_file"] = f"{name}"
            frames.append(df2)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def read_settlement_files(files) -> pd.DataFrame:
    """Settlement: CSV/ZIP/Excel (ZIP diasumsikan CSV)."""
    frames = []
    for f in files:
        name = getattr(f, "name", "uploaded")
        data = f.read()

        if name.lower().endswith(".zip"):
            zbuf = io.BytesIO(data)
            with zipfile.ZipFile(zbuf) as z:
                for zi in z.infolist():
                    if not zi.filename.lower().endswith(".csv"):
                        continue
                    with z.open(zi) as fh:
                        df = _read_csv_bytes(fh.read())
                    df["source_file"] = f"{name}:{zi.filename}"
                    frames.append(_normalize_columns(df))
        else:
            # Excel or CSV
            try:
                dfs = _read_excel_bytes_smart(data)  # pakai header detection juga
                for i, df in enumerate(dfs):
                    df["source_file"] = name
                    frames.append(_normalize_columns(df))
            except Exception:
                df = _read_csv_bytes(data)
                df["source_file"] = name
                frames.append(_normalize_columns(df))

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# ========================= Aggregations =========================

def aggregate_ticket_by_action(df: pd.DataFrame, amount_col: Optional[str]) -> pd.DataFrame:
    df = ensure_action_column(df)
    if "Action" not in df.columns or df["Action"].isna().all():
        return pd.DataFrame(columns=["Tanggal", "Tiket Detail ESPAY"])
    tmp = df.copy()
    tmp["Action_dt"] = pd.to_datetime(tmp["Action"], dayfirst=True, errors="coerce").dt.date
    tmp = tmp.dropna(subset=["Action_dt"])
    if amount_col is None:
        g = tmp.groupby("Action_dt", dropna=False).size().reset_index(name="Tiket Detail ESPAY")
    else:
        val = parse_currency_series(tmp[amount_col])
        g = tmp.assign(_val=val).groupby("Action_dt", dropna=False)["_val"].sum(min_count=1).reset_index()
        g = g.rename(columns={"_val": "Tiket Detail ESPAY"})
    g = g.rename(columns={"Action_dt": "Tanggal"}).sort_values("Tanggal").reset_index(drop=True)
    return g

def aggregate_settlement_by_date(df: pd.DataFrame, date_col: str, amount_col: str) -> pd.DataFrame:
    tmp = df.copy()
    tmp["Tanggal"] = pd.to_datetime(tmp[date_col], dayfirst=True, errors="coerce").dt.date
    val = parse_currency_series(tmp[amount_col])
    g = tmp.assign(_val=val).groupby("Tanggal", dropna=False)["_val"].sum(min_count=1).reset_index()
    g = g.rename(columns={"_val": "Settlement Dana ESPAY"})
    return g

# ========================= Streamlit UI =========================

st.set_page_config(page_title="Rekonsiliasi Tiket vs Settlement ESPAY", layout="wide")
st.title("Rekonsiliasi Tiket vs Settlement ESPAY")
st.caption("• Ticket: Excel/CSV/ZIP → 'Created'→'Action' (header auto-detect) • Settlement: CSV/ZIP/Excel.")

with st.expander("1) Upload Ticket Detail (Excel/CSV/ZIP)"):
    ticket_files = st.file_uploader(
        "Upload banyak file Ticket Detail",
        type=["xlsx","xls","csv","zip"],
        accept_multiple_files=True,
        key="ticket")
    if ticket_files:
        tickets_df = read_ticket_files(ticket_files)
        tickets_df = ensure_action_column(tickets_df)
        st.write("Preview kolom utama Ticket Detail:")
        keep = [c for c in tickets_df.columns if str(c).lower() in ["created","action","source_file"]]
        st.dataframe(tickets_df[keep].head(200), use_container_width=True)
        ticket_num_cols = guess_numeric_cols(tickets_df)
        ticket_amt = st.selectbox("Kolom nominal Ticket (opsional; kosong = hitung jumlah tiket)",
                                  options=["(hitung jumlah tiket)"] + ticket_num_cols, index=0)
        ticket_amt_col = None if ticket_amt == "(hitung jumlah tiket)" else ticket_amt
    else:
        tickets_df = pd.DataFrame()
        ticket_amt_col = None

with st.expander("2) Upload Settlement ESPAY (CSV/ZIP/Excel)"):
    settlement_files = st.file_uploader(
        "Upload banyak file Settlement",
        type=["csv","zip","xlsx","xls"],
        accept_multiple_files=True,
        key="settle")
    if settlement_files:
        settle_df = read_settlement_files(settlement_files)
        st.write("Preview Settlement (kolom tersedia):")
        st.dataframe(settle_df.head(200), use_container_width=True)
        date_candidates = guess_date_cols(settle_df)
        num_candidates  = guess_numeric_cols(settle_df)
        settle_date_col = st.selectbox("Pilih kolom TANGGAL (Settlement)", 
                                       options=date_candidates if date_candidates else settle_df.columns.tolist())
        settle_amt_col  = st.selectbox("Pilih kolom NOMINAL (Settlement)",
                                       options=num_candidates if num_candidates else settle_df.columns.tolist())
    else:
        settle_df = pd.DataFrame()
        settle_date_col = None
        settle_amt_col  = None

st.subheader("3) Periode Rekonsiliasi")
c1, c2 = st.columns(2)
with c1:
    start_date = st.date_input("Tanggal mulai", value=None)
with c2:
    end_date   = st.date_input("Tanggal akhir", value=None)

if st.button("Proses"):
    if tickets_df.empty:
        st.error("Ticket Detail belum diupload."); st.stop()
    if settle_df.empty:
        st.error("Settlement ESPAY belum diupload."); st.stop()

    tickets_df = ensure_action_column(tickets_df)
    if "Action" not in tickets_df.columns or tickets_df["Action"].isna().all():
        st.error("Tidak menemukan kolom 'Created' / tanggal yang bisa dipakai untuk membuat 'Action' pada Ticket Detail.")
        st.stop()

    tiket_agg  = aggregate_ticket_by_action(tickets_df, ticket_amt_col)
    settle_agg = aggregate_settlement_by_date(settle_df, settle_date_col, settle_amt_col)

    if start_date:
        tiket_agg  = tiket_agg[tiket_agg["Tanggal"] >= start_date]
        settle_agg = settle_agg[settle_agg["Tanggal"] >= start_date]
    if end_date:
        tiket_agg  = tiket_agg[tiket_agg["Tanggal"] <= end_date]
        settle_agg = settle_agg[settle_agg["Tanggal"] <= end_date]

    hasil = pd.merge(tiket_agg, settle_agg, on="Tanggal", how="outer").sort_values("Tanggal").reset_index(drop=True)
    for col in ["Tiket Detail ESPAY","Settlement Dana ESPAY"]:
        if col in hasil:
            hasil[col] = pd.to_numeric(hasil[col], errors="coerce").fillna(0)
    hasil["Selisih"] = hasil["Tiket Detail ESPAY"] - hasil["Settlement Dana ESPAY"]

    st.subheader("Hasil Rekonsiliasi per Tanggal")
    st.dataframe(hasil, use_container_width=True)

    with io.BytesIO() as buf:
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            tickets_df.to_excel(writer, sheet_name="All Ticket (raw+action)", index=False)
            settle_df.to_excel(writer, sheet_name="All Settlement (raw)", index=False)
            tiket_agg.to_excel(writer, sheet_name="Agg Ticket by Action", index=False)
            settle_agg.to_excel(writer, sheet_name="Agg Settlement", index=False)
            hasil.to_excel(writer, sheet_name="Rekonsiliasi per Tgl", index=False)
        st.download_button("Download Excel Hasil Rekon",
                           data=buf.getvalue(),
                           file_name="Rekon_Tiket_vs_Settlement.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
