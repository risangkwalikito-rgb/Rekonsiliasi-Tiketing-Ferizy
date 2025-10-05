# streamlit_app.py
# -*- coding: utf-8 -*-
"""Rekonsiliasi: Tiket Detail vs Settlement Dana
   - Settlement Dana ESPAY = sum(Settlement Amount/Ammount) per Transaction Date (tanpa filter bank)
   - Tiket: pilih kolom tanggal (default Created) + filter status longgar (paid/success/settle/lunas/berhasil)
   - Parser uang robust (1.095.568.800, 1,095,568,800.00, (123), 123-)
   - Baca cepat: CSV C-engine, Excel peek 25 baris + usecols, cache per file
"""

from __future__ import annotations

import io, re, zipfile, calendar
from typing import Optional, List, Tuple, Iterable
import numpy as np, pandas as pd, streamlit as st
from dateutil import parser as dtparser

# ========= Parsers & helpers =========

def _to_num(sr: pd.Series) -> pd.Series:
    if sr.empty: return sr.astype(float)
    def parse_one(x: str) -> float:
        if x is None: return 0.0
        s = str(x).strip().lower()
        if s in ("", "nan", "none"): return 0.0
        neg = False
        if s.startswith("(") and s.endswith(")"): neg, s = True, s[1:-1].strip()
        if s.endswith("-"): neg, s = True, s[:-1].strip()
        s = re.sub(r"(idr|rp|cr|dr)", "", s)
        s = re.sub(r"[^0-9\.,\-]", "", s)
        if not s or s == "-": return 0.0
        last_dot, last_com = s.rfind("."), s.rfind(",")
        if last_dot == -1 and last_com == -1:
            num = float(s)
        elif last_dot != -1 and last_com != -1:
            if last_dot > last_com:  # '.' decimal
                num = float(s.replace(",", ""))
            else:                    # ',' decimal
                num = float(s.replace(".", "").replace(",", "."))
        else:
            sep = "." if last_dot != -1 else ","
            if s.count(sep) > 1:
                num = float(s.replace(sep, ""))            # thousands
            else:
                frac_len = len(s) - (s.rfind(sep) + 1)
                if 1 <= frac_len <= 2: num = float(s.replace(sep, "."))  # decimal
                else: num = float(s.replace(sep, ""))                    # thousands
        return -num if neg else num
    return sr.apply(parse_one).astype(float)

def _to_datetime(val) -> Optional[pd.Timestamp]:
    if pd.isna(val): return None
    if isinstance(val, (int, float, np.number)):
        if np.isfinite(val) and 1 <= float(val) <= 100000:
            base = pd.Timestamp("1899-12-30")
            try: return base + pd.to_timedelta(float(val), unit="D")
            except Exception: return None
        return None
    if isinstance(val, (pd.Timestamp, np.datetime64)): return pd.to_datetime(val)
    s = str(val).strip()
    if not s: return None
    for dayfirst in (True, False):
        try: return pd.Timestamp(dtparser.parse(s, dayfirst=dayfirst, fuzzy=True))
        except Exception: continue
    return None

def _to_datetime_series(sr: pd.Series) -> pd.Series:
    if sr.empty: return pd.to_datetime(pd.Series([], dtype=str), errors="coerce")
    dt = pd.to_datetime(sr, errors="coerce", dayfirst=True, infer_datetime_format=True)
    mask = dt.isna()
    if mask.any():
        dt2 = pd.to_datetime(sr[mask], errors="coerce", dayfirst=False, infer_datetime_format=True)
        dt = dt.where(~mask, dt2)
    num = pd.to_numeric(sr, errors="coerce")
    mask_serial = num.between(1, 100000)
    if mask_serial.any():
        base = pd.Timestamp("1899-12-30")
        dt = dt.where(~mask_serial, base + pd.to_timedelta(num[mask_serial], unit="D"))
    return dt

def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    if df.empty: return None
    cols = [c for c in df.columns if isinstance(c, str)]
    norm = {c.lower().strip(): c for c in cols}
    for n in candidates:
        k = n.lower().strip()
        if k in norm: return norm[k]
    for n in candidates:
        key = n.lower().strip()
        for c in cols:
            if key in c.lower(): return c
    return None

def _idr_fmt(n: float) -> str:
    if pd.isna(n): return "-"
    s = f"{abs(int(round(n))):,}".replace(",", ".")
    return f"({s})" if n < 0 else s

def _norm_label(s: str) -> str:
    if s is None or (isinstance(s, float) and np.isnan(s)): return ""
    return re.sub(r"\s+", " ", str(s).strip().lower())

# ========= Readers (cache, zip, header guess) =========

SUPPORTED_EXTS = (".xlsx", ".xls", ".xlsb", ".csv", ".zip")

@st.cache_data(show_spinner=False)
def _bytes_of(uploaded_file) -> bytes:
    uploaded_file.seek(0); data = uploaded_file.read(); uploaded_file.seek(0); return data

def _read_csv_fast(buf: io.BytesIO) -> pd.DataFrame:
    try: buf.seek(0); return pd.read_csv(buf, dtype=str, na_filter=False)
    except Exception: ...
    try: buf.seek(0); return pd.read_csv(buf, sep=";", dtype=str, na_filter=False)
    except Exception: ...
    buf.seek(0); return pd.read_csv(buf, engine="python", sep=None, dtype=str, na_filter=False)

def _read_excel_by_ext(buf: io.BytesIO, name: str, *, header=None, nrows=None, usecols=None) -> pd.DataFrame:
    low = name.lower(); kw = dict(dtype=str, na_filter=False, header=header, nrows=nrows, usecols=usecols)
    if low.endswith(".xlsb"):  return pd.read_excel(buf, engine="pyxlsb", **kw)
    if low.endswith(".xlsx"):  return pd.read_excel(buf, engine="openpyxl", **kw)
    if low.endswith(".xls"):   return pd.read_excel(buf, engine="xlrd", **kw)
    raise ValueError(f"Ekstensi tidak didukung: {name}")

def _extract_zip_bytes(data: bytes) -> list[tuple[str, io.BytesIO]]:
    out = []
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for info in zf.infolist():
            if info.is_dir(): continue
            nm = info.filename
            if nm.lower().endswith((".xlsx", ".xls", ".xlsb", ".csv")):
                with zf.open(info) as f: out.append((nm, io.BytesIO(f.read())))
    return out

def _guess_header_row(df_no_header: pd.DataFrame, targets: Iterable[str]) -> int:
    scan = min(25, len(df_no_header)); best_row, best = 0, -1
    for i in range(scan):
        row = df_no_header.iloc[i].astype(str).str.lower().str.strip().fillna("")
        score = sum(1 for t in targets if t in " ".join(row.tolist()))
        if score > best: best_row, best = i, score
        if score >= 4: break
    return best_row

# ---- Tiket readers ----
def _read_tiket_from_bytes(buf: io.BytesIO, name: str) -> pd.DataFrame:
    low = name.lower()
    if low.endswith(".csv"):
        df = _read_csv_fast(buf); df["__source__"] = name; return df
    peek_targets = ["created","tarif","st bayar","status","bank","channel","payment","bayar"]
    buf.seek(0); peek = _read_excel_by_ext(buf, name, header=None, nrows=25)
    if peek.empty: return pd.DataFrame()
    header_row = _guess_header_row(peek, peek_targets)
    need_keys = ["created","create date","created date","created (wib)","created time","tanggal",
                 "paid","payment","bayar","paid time","paid date","payment time",
                 "tarif","nominal","amount","total","harga",
                 "st bayar","status bayar","status",
                 "bank","payment channel","channel","payment method","bank/ewallet"]
    usecols = lambda c: any(k in str(c).lower() for k in need_keys)
    buf.seek(0); df = _read_excel_by_ext(buf, name, header=header_row, usecols=usecols)
    if len(df.columns)==0: buf.seek(0); df = _read_excel_by_ext(buf, name, header=header_row)
    df["__source__"] = name; return df

# ---- Settlement readers ----
SETTLE_TARGETS = ["transaction date","trans date","tanggal transaksi","tgl transaksi","tanggal trans","tgl trans",
                  "settlement date","settlementdate","tanggal settlement","tgl settlement",
                  "settlement ammount","settlement amount","amount settlement","nominal settlement",
                  "amount","nominal","jumlah","total amount","net settlement amount","net settlement",
                  "product name","product","productname","nama produk"]
def _read_settle_from_bytes(buf: io.BytesIO, name: str) -> pd.DataFrame:
    low = name.lower()
    if low.endswith(".csv"):
        df = _read_csv_fast(buf)
        if len(df.columns)==0:
            buf.seek(0); peek = pd.read_csv(buf, engine="python", sep=None, header=None, nrows=25, dtype=str, na_filter=False)
            header_row = _guess_header_row(peek, SETTLE_TARGETS)
            buf.seek(0); df = pd.read_csv(buf, engine="python", sep=None, header=header_row, dtype=str, na_filter=False)
        df["__source__"]=name; return df
    buf.seek(0); peek = _read_excel_by_ext(buf, name, header=None, nrows=25)
    if peek.empty: return pd.DataFrame()
    header_row = _guess_header_row(peek, SETTLE_TARGETS)
    usecols = lambda c: any(k in str(c).lower() for k in SETTLE_TARGETS)
    buf.seek(0); df = _read_excel_by_ext(buf, name, header=header_row, usecols=usecols)
    if len(df.columns)==0: buf.seek(0); df = _read_excel_by_ext(buf, name, header=header_row)
    df["__source__"]=name; return df

@st.cache_data(show_spinner=False)
def _parse_tiket_any_cached(data: bytes, name: str) -> pd.DataFrame:
    if name.lower().endswith(".zip"):
        frames = [_read_tiket_from_bytes(b, n) for n,b in _extract_zip_bytes(data)]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return _read_tiket_from_bytes(io.BytesIO(data), name)

@st.cache_data(show_spinner=False)
def _parse_settle_any_cached(data: bytes, name: str) -> pd.DataFrame:
    if name.lower().endswith(".zip"):
        frames = [_read_settle_from_bytes(b, n) for n,b in _extract_zip_bytes(data)]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return _read_settle_from_bytes(io.BytesIO(data), name)

def _concat_tiket_files(files) -> pd.DataFrame:
    frames=[]; 
    for f in (files or []):
        df=_parse_tiket_any_cached(_bytes_of(f), f.name)
        if not df.empty: frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def _concat_settle_files(files) -> pd.DataFrame:
    frames=[]; 
    for f in (files or []):
        df=_parse_settle_any_cached(_bytes_of(f), f.name)
        if not df.empty: frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# ========= Business helpers =========

def _month_selector() -> Tuple[int,int]:
    from datetime import date
    today=date.today()
    years=list(range(today.year-5, today.year+2))
    months=[("01","Januari"),("02","Februari"),("03","Maret"),("04","April"),("05","Mei"),("06","Juni"),
            ("07","Juli"),("08","Agustus"),("09","September"),("10","Oktober"),("11","November"),("12","Desember")]
    c1,c2 = st.columns(2)
    with c1: year=st.selectbox("Tahun", years, index=years.index(today.year))
    with c2: sel=st.selectbox("Bulan", months, index=int(today.strftime("%m"))-1, format_func=lambda x:x[1]); month=int(sel[0])
    return year, month

def _derive_action_date_from_created(sr: pd.Series, zone: str, *, adjust_midnight: bool=True) -> pd.Series:
    dt=_to_datetime_series(sr); base=dt.dt.normalize()
    if not adjust_midnight: return base
    z=zone.upper(); minus=1 if "WITA" in z else (2 if "WIT" in z else 0)
    if minus==0: return base
    shift = pd.to_timedelta((dt.dt.hour.eq(0)).astype(int)*minus, unit="D")
    return base - shift

# ========= App =========

st.set_page_config(page_title="Rekonsiliasi Tiket vs Settlement", layout="wide")
st.title("Rekonsiliasi: Tiket Detail vs Settlement Dana")

with st.sidebar:
    st.header("1) Upload Sumber (multi-file)")
    tiket_files = st.file_uploader("Tiket Detail (.csv/.xls/.xlsx/.xlsb/.zip)",
                                   type=["csv","xls","xlsx","xlsb","zip"], accept_multiple_files=True)
    settle_files = st.file_uploader("Settlement Dana (.csv/.xls/.xlsx/.xlsb/.zip)",
                                    type=["csv","xls","xlsx","xlsb","zip"], accept_multiple_files=True)

# gabung (cached)
tiket_df  = _concat_tiket_files(tiket_files)
settle_df = _concat_settle_files(settle_files)

with st.sidebar:
    st.header("2) Parameter Bulan & Tahun (WAJIB)")
    y,m = _month_selector()
    month_start = pd.Timestamp(y,m,1)
    month_end   = pd.Timestamp(y,m,calendar.monthrange(y,m)[1])
    st.caption(f"Periode: {month_start.date()} s/d {month_end.date()}")

    st.header("3) Zona Waktu Cabang")
    zone = st.selectbox("Zona waktu", ["WIB (UTC+7)","WITA (UTC+8)","WIT (UTC+9)"], index=0)
    adjust_midnight = st.checkbox("Koreksi jam 00 (WITA −1 hari, WIT −2 hari)", value=False)

    st.header("4) Opsi")
    # NEW: pilih kolom tanggal tiket
    t_date_choice = None
    if not tiket_df.empty:
        date_candidates = [c for c in tiket_df.columns if any(k in str(c).lower() for k in
                          ["created","create date","created date","created (wib)","created time","tanggal",
                           "paid","payment","bayar","paid time","paid date","payment time"])]
        date_candidates = list(dict.fromkeys(date_candidates))  # unique, keep order
        if date_candidates:
            t_date_choice = st.selectbox("Tanggal acuan Tiket", options=date_candidates, index=0, help="Pilih kolom tanggal untuk agregasi tiket (default: Created).")
    show_charts = st.checkbox("Tampilkan grafik ringkas", value=True)

    go = st.button("Proses", type="primary", use_container_width=True)

if go:
    if not tiket_files:
        st.error("Harap upload **Tiket Detail** minimal 1 file."); st.stop()

    # ---- Tiket ----
    t_created_default = _find_col(tiket_df, ["Created","Created Date","Create Date","Tanggal Buat","Created (WIB)","Created Time","Tanggal"])
    t_amt  = _find_col(tiket_df, ["tarif","nominal","amount","total","harga"])
    t_stat = _find_col(tiket_df, ["St Bayar","Status Bayar","status bayar","status"])
    t_bank = _find_col(tiket_df, ["Bank","Payment Channel","channel","payment method","bank/ewallet"])

    missing=[]
    if (t_date_choice or t_created_default) is None: missing.append("Tiket Detail: kolom tanggal (Created / Paid / Payment)")
    if t_amt is None:  missing.append("Tiket Detail: tarif/nominal")
    if t_stat is None: missing.append("Tiket Detail: St Bayar/Status")
    if t_bank is None: missing.append("Tiket Detail: Bank/Channel")
    if missing:
        st.error("Kolom wajib tidak ditemukan → " + "; ".join(missing))
        st.write("Kolom Tiket tersedia:", list(tiket_df.columns)); st.stop()

    ticket_date_col = t_date_choice or t_created_default

    td = tiket_df.copy()
    td["__action_date"] = _derive_action_date_from_created(td[ticket_date_col], zone, adjust_midnight=adjust_midnight)
    td = td[~td["__action_date"].isna()]

    # filter status longgar + bank espay (fuzzy)
    td_stat_v = td[t_stat].astype(str).str.strip().str.lower()
    ok   = td_stat_v.str.contains(r"paid|success|settle|lunas|berhasil", regex=True)
    bad  = td_stat_v.str.contains(r"unpaid|pending|gagal|cancel|void|expired|refund", regex=True)
    td_bank_v = td[t_bank].astype(str).str.strip().str.lower()
    bank_mask = td_bank_v.str.contains("espay")

    td = td[ ok & ~bad & bank_mask ]
    td = td[(td["__action_date"] >= month_start) & (td["__action_date"] <= month_end)]
    td[t_amt] = _to_num(td[t_amt])

    tiket_by_date = td.groupby(td["__action_date"])[t_amt].sum()
    tiket_by_date.index = pd.to_datetime(tiket_by_date.index).date

    # ---- Settlement (TXN DATE + AMOUNT only) ----
    s_txn_date = _find_col(settle_df, ["Transaction Date","Trans Date","Tanggal Transaksi","Tgl Transaksi","Tanggal Trans","Tgl Trans"])
    s_amt = _find_col(settle_df, ["Settlement Ammount","Settlement Amount","Amount Settlement","Nominal Settlement","Amount","Nominal","Jumlah","Total Amount","Net Settlement Amount","Net Settlement"])
    s_settle_date = _find_col(settle_df, ["Settlement Date","SettlementDate","Tanggal Settlement","Tgl Settlement"])
    s_prod = _find_col(settle_df, ["Product Name","Product","ProductName","Nama Produk"])

    miss2=[]
    if s_txn_date is None: miss2.append("Settlement: Transaction Date")
    if s_amt is None:      miss2.append("Settlement: Settlement Amount/Ammount")
    if miss2:
        st.error("Kolom wajib tidak ditemukan → " + "; ".join(miss2))
        st.write("Kolom Settlement tersedia:", list(settle_df.columns)); st.stop()

    sd_txn = settle_df.copy()
    sd_txn[s_txn_date] = _to_datetime_series(sd_txn[s_txn_date]).dt.normalize()
    sd_txn = sd_txn[~sd_txn[s_txn_date].isna()]
    sd_txn = sd_txn[(sd_txn[s_txn_date] >= month_start) & (sd_txn[s_txn_date] <= month_end)]
    sd_txn[s_amt] = _to_num(sd_txn[s_amt])
    settle_total = sd_txn.groupby(sd_txn[s_txn_date])[s_amt].sum()
    settle_total.index = pd.to_datetime(settle_total.index).date

    # BCA/Non-BCA opsional
    if s_settle_date is not None and s_prod is not None:
        sd_settle = settle_df.copy()
        sd_settle[s_settle_date] = _to_datetime_series(sd_settle[s_settle_date]).dt.normalize()
        sd_settle = sd_settle[~sd_settle[s_settle_date].isna()]
        sd_settle = sd_settle[(sd_settle[s_settle_date] >= month_start) & (sd_settle[s_settle_date] <= month_end)]
        sd_settle[s_amt] = _to_num(sd_settle[s_amt])
        target = _norm_label("BCA VA Online")
        prod_norm = sd_settle[s_prod].apply(_norm_label)
        bca_mask = prod_norm.eq(target)
        settle_bca    = sd_settle[bca_mask].groupby(sd_settle[bca_mask][s_settle_date])[s_amt].sum() if bca_mask.any() else pd.Series(dtype=float)
        settle_nonbca = sd_settle[~bca_mask].groupby(sd_settle[~bca_mask][s_settle_date])[s_amt].sum() if (~bca_mask).any() else pd.Series(dtype=float)
    else:
        settle_bca = pd.Series(dtype=float); settle_nonbca = pd.Series(dtype=float)

    # ---- Reindex & table ----
    idx = pd.Index(pd.date_range(month_start, month_end, freq="D").date, name="Tanggal")
    def _reidx(s: pd.Series) -> pd.Series:
        if not isinstance(s, pd.Series): s = pd.Series(dtype=float)
        if len(getattr(s, "index", [])): s.index = pd.to_datetime(s.index).date
        return s.reindex(idx, fill_value=0.0)

    final = pd.DataFrame(index=idx)
    final["Tiket Detail ESPAY"]    = _reidx(tiket_by_date).values
    final["Settlement Dana ESPAY"] = _reidx(settle_total).values
    final["Selisih"]               = final["Tiket Detail ESPAY"] - final["Settlement Dana ESPAY"]
    final["Settlement Dana BCA"]     = _reidx(settle_bca).values
    final["Settlement Dana Non BCA"] = _reidx(settle_nonbca).values

    view = final.reset_index(); view.insert(0, "No", range(1, len(view)+1))
    total_row = pd.DataFrame([{
        "No":"", "Tanggal":"TOTAL",
        "Tiket Detail ESPAY": final["Tiket Detail ESPAY"].sum(),
        "Settlement Dana ESPAY": final["Settlement Dana ESPAY"].sum(),
        "Selisih": final["Selisih"].sum(),
        "Settlement Dana BCA": final["Settlement Dana BCA"].sum(),
        "Settlement Dana Non BCA": final["Settlement Dana Non BCA"].sum(),
    }])
    view_total = pd.concat([view, total_row], ignore_index=True)

    fmt = view_total.copy()
    for c in ["Tiket Detail ESPAY","Settlement Dana ESPAY","Selisih","Settlement Dana BCA","Settlement Dana Non BCA"]:
        fmt[c] = fmt[c].apply(_idr_fmt)

    st.subheader("Hasil Rekonsiliasi per Tanggal (mengikuti bulan parameter)")
    st.dataframe(fmt, use_container_width=True, hide_index=True)

    if show_charts:
        st.subheader("Grafik Ringkas")
        chart_data = view[view["Tanggal"] != "TOTAL"].set_index("Tanggal")[
            ["Tiket Detail ESPAY","Settlement Dana ESPAY","Settlement Dana BCA","Settlement Dana Non BCA"]
        ]
        st.bar_chart(chart_data)

    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        view_total.to_excel(xw, index=False, sheet_name="Rekonsiliasi")
        fmt.to_excel(xw, index=False, sheet_name="Rekonsiliasi_View")
    st.download_button("Unduh Excel", data=out.getvalue(),
        file_name=f"rekonsiliasi_{y}-{m:02d}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True)
