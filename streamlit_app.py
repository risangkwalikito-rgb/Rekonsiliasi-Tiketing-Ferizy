# app.py
import io
import zipfile
import calendar
from datetime import date
import pandas as pd
import numpy as np
import streamlit as st

st.set_page_config(page_title="Tabel Rekon Otomatis - Ferizy", layout="wide")
st.title("Detail Tiket from Payment Report Ferizy")
st.caption(
    "Upload Payment Report (Excel/CSV/ZIP) di sidebar kiri. "
    "Aplikasi menambahkan kolom **Tanggal** (dari **kolom B**, tanpa jam), "
    "menjumlahkan **nominal** dari **kolom K** untuk setiap kanal, "
    "menyediakan parameter **bulan/tahun** sehingga **Tabel Harian** otomatis 1–28/29/30/31, "
    "kolom **Total** di semua rekap, dan baris **Sub Total**. "
    "Tampilan angka: format Indonesia (ribuan titik)."
)

# =========================
# Helpers & Constants
# =========================
CHANNEL_COLS = ["Cash", "Prepaid - BRI", "Prepaid - Mandiri", "Prepaid - BNI",
                "Prepaid - BCA", "SKPT", "IFCS", "Redeem", "ESPAY", "FINNET"]

def resolve_column(df: pd.DataFrame, letter: str, pos_index: int, fallback_contains=None):
    for c in df.columns:
        if str(c).strip().lower() == letter.lower():
            return c, f"named '{letter}'"
    if fallback_contains:
        for c in df.columns:
            if fallback_contains.lower() in str(c).strip().lower():
                return c, f"semantic match contains '{fallback_contains}'"
    if 0 <= pos_index < len(df.columns):
        return df.columns[pos_index], f"position index {pos_index} ({letter})"
    return None, "missing"

def normalize_str_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower()

def format_id_number(v, decimals: int = 0):
    if pd.isna(v):
        return ""
    try:
        n = float(v)
    except Exception:
        return v
    s = f"{n:,.{decimals}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")

def df_format_id(df: pd.DataFrame, cols, decimals: int = 0) -> pd.DataFrame:
    disp = df.copy()
    for c in cols:
        if c in disp.columns:
            disp[c] = disp[c].apply(lambda x: format_id_number(x, decimals))
    return disp

def _read_csv_bytes(b: bytes) -> pd.DataFrame:
    bio = io.BytesIO(b)
    try:
        import pyarrow  # noqa: F401
        return pd.read_csv(bio, engine="pyarrow")
    except Exception:
        bio.seek(0)
        try:
            return pd.read_csv(bio, engine="python", on_bad_lines="skip", low_memory=False)
        except Exception:
            bio.seek(0)
            return pd.read_csv(bio, low_memory=False)

def _read_excel_bytes(b: bytes, sheet_name=None) -> pd.DataFrame:
    bio = io.BytesIO(b)
    xl = pd.ExcelFile(bio)
    target = sheet_name if (sheet_name in xl.sheet_names) else xl.sheet_names[0]
    return xl.parse(target, dtype=object)

@st.cache_data(show_spinner=False)
def read_single_file(uploaded_name: str, b: bytes, sheet=None):
    name = uploaded_name.lower()
    if name.endswith((".xlsx", ".xls")):
        df = _read_excel_bytes(b, sheet_name=sheet)
        sheets = pd.ExcelFile(io.BytesIO(b)).sheet_names
        chosen = sheet if (sheet in sheets) else sheets[0]
        return df, sheets, chosen, [{"file": uploaded_name, "type": "excel", "sheet": chosen}]
    elif name.endswith(".csv"):
        df = _read_csv_bytes(b)
        return df, None, None, [{"file": uploaded_name, "type": "csv"}]
    else:
        return None, None, None, []

@st.cache_data(show_spinner=False)
def read_zip(archive_bytes: bytes):
    zf = zipfile.ZipFile(io.BytesIO(archive_bytes))
    frames, manifest = [], []
    for info in zf.infolist():
        if info.is_dir(): 
            continue
        fname = info.filename
        lower = fname.lower()
        try:
            with zf.open(info) as f:
                content = f.read()
            if lower.endswith(".csv"):
                df = _read_csv_bytes(content)
                frames.append(df)
                manifest.append({"file": fname, "type": "csv"})
            elif lower.endswith((".xlsx", ".xls")):
                try:
                    xl = pd.ExcelFile(io.BytesIO(content))
                    sheet = xl.sheet_names[0]
                    df = xl.parse(sheet, dtype=object)
                    frames.append(df)
                    manifest.append({"file": fname, "type": "excel", "sheet": sheet})
                except Exception as e:
                    manifest.append({"file": fname, "type": "excel", "error": str(e)})
            else:
                manifest.append({"file": fname, "type": "skipped"})
        except Exception as e:
            manifest.append({"file": fname, "type": "unknown", "error": str(e)})
    df_all = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
    return df_all, manifest

def build_metrics(df, h_col, aa_col=None, amount_col=None):
    cols_order = CHANNEL_COLS.copy()
    if df.empty:
        out = pd.DataFrame({c: [0.0] for c in cols_order})
        out["Total"] = out.sum(axis=1)
        return out

    h_vals = normalize_str_series(df[h_col])

    def metric_for(mask):
        sub = df[mask] if (mask is not None and mask.any()) else df.iloc[0:0]
        if amount_col and amount_col in df.columns:
            vals = pd.to_numeric(sub[amount_col], errors='coerce')
            return float(vals.sum(skipna=True))
        else:
            return float(len(sub))

    cash_mask = h_vals.eq('cash')
    data = {
        'Cash': metric_for(cash_mask),
        'Prepaid - BRI': metric_for(h_vals.eq('prepaid-bri')),
        'Prepaid - Mandiri': metric_for(h_vals.eq('prepaid-mandiri')),
        'Prepaid - BNI': metric_for(h_vals.eq('prepaid-bni')),
        'Prepaid - BCA': metric_for(h_vals.eq('prepaid-bca')),
        'SKPT': metric_for(h_vals.eq('skpt')),
        'IFCS': metric_for(cash_mask),  # IFCS = cash
        'Redeem': metric_for(h_vals.eq('redeem')),
        'ESPAY': 0.0,
        'FINNET': 0.0,
    }
    if aa_col is not None and aa_col in df.columns:
        aa_vals = normalize_str_series(df[aa_col])
        data['ESPAY']  = metric_for(h_vals.eq('finpay') & aa_vals.str.contains('esp', na=False))
        data['FINNET'] = metric_for(h_vals.eq('finpay') & ~aa_vals.str.contains('esp', na=False))

    out = pd.DataFrame([data], columns=cols_order)
    out["Total"] = out[cols_order].sum(axis=1)
    return out

def build_daily_table(df_month, year_sel, month_sel, h_col, aa_col, amount_col, date_col='Tanggal'):
    last_day = calendar.monthrange(year_sel, month_sel)[1]
    all_days = pd.date_range(f"{year_sel}-{month_sel:02d}-01", periods=last_day, freq='D').date
    result = pd.DataFrame({"Tanggal": all_days})

    if df_month.empty:
        for c in CHANNEL_COLS: result[c] = 0.0
        result["Total"] = 0.0
        return result

    h_vals = normalize_str_series(df_month[h_col])
    aa_vals = normalize_str_series(df_month[aa_col]) if (aa_col is not None and aa_col in df_month.columns) else pd.Series([None] * len(df_month))
    amt = pd.to_numeric(df_month[amount_col], errors='coerce').fillna(0.0)
    tgl = pd.to_datetime(df_month[date_col], errors='coerce').dt.date

    mask = {
        "Cash": h_vals.eq('cash'),
        "Prepaid - BRI": h_vals.eq('prepaid-bri'),
        "Prepaid - Mandiri": h_vals.eq('prepaid-mandiri'),
        "Prepaid - BNI": h_vals.eq('prepaid-bni'),
        "Prepaid - BCA": h_vals.eq('prepaid-bca'),
        "SKPT": h_vals.eq('skpt'),
        "IFCS": h_vals.eq('cash'),
        "Redeem": h_vals.eq('redeem'),
        "ESPAY": (h_vals.eq('finpay') & aa_vals.str.contains('esp', na=False)) if (aa_col is not None and aa_col in df_month.columns) else pd.Series([False]*len(df_month)),
        "FINNET": (h_vals.eq('finpay') & ~aa_vals.str.contains('esp', na=False)) if (aa_col is not None and aa_col in df_month.columns) else pd.Series([False]*len(df_month)),
    }

    for key, m in mask.items():
        s = pd.Series(np.where(m, amt, 0.0)).groupby(tgl).sum()
        s = s.reindex(all_days, fill_value=0.0)
        result[key] = s.values

    result["Total"] = result[CHANNEL_COLS].sum(axis=1)
    return result

def build_daily_total_only(df_month, year_sel, month_sel, amount_col, date_col='Tanggal'):
    """Cepat: hanya Tanggal & Total (tanpa pecahan kanal)."""
    last_day = calendar.monthrange(year_sel, month_sel)[1]
    all_days = pd.date_range(f"{year_sel}-{month_sel:02d}-01", periods=last_day, freq='D').date
    result = pd.DataFrame({"Tanggal": all_days})
    if df_month.empty:
        result["Total"] = 0.0
        return result
    amt = pd.to_numeric(df_month[amount_col], errors='coerce').fillna(0.0)
    tgl = pd.to_datetime(df_month[date_col], errors='coerce').dt.date
    s = pd.Series(amt).groupby(tgl).sum()
    result["Total"] = s.reindex(all_days, fill_value=0.0).values
    return result

def filter_port(df, q_col, port_name):
    q_vals = normalize_str_series(df[q_col])
    return df[q_vals.eq(port_name.strip().lower())]

# =========================
# GLOBAL INIT & SIDEBAR
# =========================
df = None
error_detail = st.sidebar.checkbox("Tampilkan detail error di halaman (debug)", value=False)

with st.sidebar:
    st.header("📤 Upload & Parameter")
    uploaded = st.file_uploader("Upload Payment Report (Excel/CSV/ZIP)",
                                type=["xlsx", "xls", "csv", "zip"])

    sheet_choice = None
    manifest_info = None

    if uploaded:
        try:
            name = uploaded.name.lower()
            data_bytes = uploaded.getvalue()

            if name.endswith(".zip"):
                df, manifest_info = read_zip(data_bytes)
                st.caption(f"ZIP terdeteksi. Tergabung: {len(df)} baris dari {len(manifest_info)} file.")
                with st.expander("Daftar isi ZIP"):
                    st.write(manifest_info)
            else:
                df_tmp, sheets, chosen_sheet, mf = read_single_file(uploaded.name, data_bytes, sheet=None)
                manifest_info = mf
                if sheets:
                    sheet_choice = st.selectbox("Pilih sheet:", sheets, index=sheets.index(chosen_sheet) if chosen_sheet in sheets else 0)
                    df, _, _, _ = read_single_file(uploaded.name, data_bytes, sheet=sheet_choice)
                else:
                    df = df_tmp
                st.caption(f"File: {uploaded.name} | Baris: {len(df)}")
        except Exception as e:
            st.error("Gagal membaca file yang diunggah.")
            if error_detail:
                st.exception(e)

    # Parameter Bulan/Tahun
    st.markdown("---")
    st.subheader("🗓️ Periode")
    try:
        if uploaded is not None and isinstance(df, pd.DataFrame) and not df.empty:
            b_col, _ = resolve_column(df, 'B', 1)
            if b_col is not None and b_col in df.columns:
                df['Tanggal'] = pd.to_datetime(df[b_col], errors='coerce').dt.date
            else:
                df['Tanggal'] = pd.NaT

            if df['Tanggal'].notna().any():
                dmin = pd.to_datetime(df['Tanggal']).min()
                dmax = pd.to_datetime(df['Tanggal']).max()
                years = list(range(int(dmin.year), int(dmax.year) + 1))
                default_year = int(dmax.year)
                default_month = int(dmax.month)
            else:
                today = date.today()
                years = [today.year]
                default_year = today.year
                default_month = today.month
        else:
            today = date.today()
            years = [today.year]
            default_year = today.year
            default_month = today.month
    except Exception:
        today = date.today()
        years = [today.year]
        default_year = today.year
        default_month = today.month
        if error_detail:
            st.warning("Gagal mendeteksi rentang tanggal; memakai tahun/bulan saat ini.")

    bulan_id = ["Januari","Februari","Maret","April","Mei","Juni","Juli","Agustus","September","Oktober","November","Desember"]
    year_sel = st.selectbox("Tahun", years, index=min(years.index(default_year), len(years)-1))
    month_sel_name = st.selectbox("Bulan", bulan_id, index=max(0, min(default_month-1, 11)))
    month_sel = bulan_id.index(month_sel_name) + 1

# =========================
# MAIN (dibungkus try/except)
# =========================
try:
    if not uploaded:
        st.info("Silakan upload file di sidebar kiri untuk memulai.")
        st.stop()

    if df is None or df.empty:
        st.warning("Tidak ada data yang bisa dibaca dari file yang diunggah.")
        st.stop()

    st.write(":small_blue_diamond: Baris data terunggah:", len(df))

    # Pemetaan kolom (diam—tanpa tampil expander)
    h_col, _ = resolve_column(df, 'H', 7)
    k_col, _ = resolve_column(df, 'K', 10)
    aa_col, _ = resolve_column(df, 'AA', 26)
    q_col, _ = resolve_column(df, 'Q', 16)
    b_col, _ = resolve_column(df, 'B', 1)

    if h_col is None:
        st.error("Kolom H (kanal) tidak ditemukan.")
        st.stop()

    if 'Tanggal' not in df.columns:
        if b_col is not None and b_col in df.columns:
            df['Tanggal'] = pd.to_datetime(df[b_col], errors='coerce').dt.date
        else:
            df['Tanggal'] = pd.NaT

    # Filter bulan/tahun
    if df['Tanggal'].notna().any():
        df_valid = df[df['Tanggal'].notna()].copy()
        df_valid['Tanggal_ts'] = pd.to_datetime(df_valid['Tanggal'])
        df_month = df_valid[
            (df_valid['Tanggal_ts'].dt.year == year_sel) &
            (df_valid['Tanggal_ts'].dt.month == month_sel)
        ].copy()
    else:
        df_month = df.iloc[0:0].copy()

    # ===== Tabel Harian (All Ports) + Sub Total =====
    st.subheader(f"Tabel Harian — {bulan_id[month_sel-1]} {year_sel} (sum kolom K) + Total + Sub Total")
    daily_table = build_daily_table(
        df_month=df_month,
        year_sel=year_sel, month_sel=month_sel,
        h_col=h_col, aa_col=aa_col, amount_col=k_col, date_col='Tanggal'
    )
    subtotal_vals = daily_table[CHANNEL_COLS + ["Total"]].sum(numeric_only=True)
    daily_with_sub = pd.concat(
        [daily_table, pd.DataFrame([{"Tanggal": "Sub Total", **subtotal_vals.to_dict()}])],
        ignore_index=True
    )
    st.dataframe(df_format_id(daily_with_sub, cols=CHANNEL_COLS + ["Total"], decimals=0), use_container_width=True)
    st.download_button(
        "Unduh Tabel Harian (CSV)",
        daily_with_sub.to_csv(index=False).encode("utf-8"),
        file_name=f"rekon_harian_{year_sel}_{month_sel:02d}.csv",
        mime="text/csv"
    )

    # ===== Rekap Bulanan (Semua Pelabuhan) + Total =====
    st.subheader("Rekap Bulanan — Semua Pelabuhan (sum kolom K) + Total")
    main_metrics_month = build_metrics(df_month, h_col=h_col, aa_col=aa_col, amount_col=k_col)
    st.dataframe(df_format_id(main_metrics_month, cols=CHANNEL_COLS + ["Total"], decimals=0), use_container_width=True)
    st.download_button(
        "Unduh Rekap Bulanan (CSV)",
        main_metrics_month.to_csv(index=False).encode('utf-8'),
        file_name=f"rekap_bulanan_{year_sel}_{month_sel:02d}.csv",
        mime="text/csv"
    )

    # ===== Per Pelabuhan: tampilkan PER TANGGAL =====
    if q_col is not None and not df_month.empty:
        st.subheader("Tabel Per Pelabuhan (bulan terpilih) — Per Tanggal")
        tabs = st.tabs(["Merak", "Bakauheni", "Ketapang"])
        for tab, port in zip(tabs, ["merak", "bakauheni", "ketapang"]):
            with tab:
                port_df = filter_port(df_month, q_col, port)

                # 1) Cepat: Tanggal & Total saja
                daily_total = build_daily_total_only(
                    df_month=port_df,
                    year_sel=year_sel, month_sel=month_sel,
                    amount_col=k_col, date_col='Tanggal'
                )
                st.markdown(f"**Total Harian {port.title()}**")
                st.dataframe(df_format_id(daily_total, cols=["Total"], decimals=0), use_container_width=True)
                st.download_button(
                    f"Unduh Total Harian {port.title()} (CSV)",
                    daily_total.to_csv(index=False).encode('utf-8'),
                    file_name=f"rekon_{port}_total_harian_{year_sel}_{month_sel:02d}.csv",
                    mime="text/csv"
                )

                # 2) Opsional (lebih detail): per kanal per tanggal
                show_detail = st.checkbox(f"Tampilkan rincian kanal {port.title()} (lebih berat)", value=False, key=f"detail_{port}")
                if show_detail:
                    daily_port_detail = build_daily_table(
                        df_month=port_df,
                        year_sel=year_sel, month_sel=month_sel,
                        h_col=h_col, aa_col=aa_col, amount_col=k_col, date_col='Tanggal'
                    )
                    st.dataframe(df_format_id(daily_port_detail, cols=CHANNEL_COLS + ["Total"], decimals=0), use_container_width=True)
                    st.download_button(
                        f"Unduh Harian (dengan kanal) {port.title()} (CSV)",
                        daily_port_detail.to_csv(index=False).encode('utf-8'),
                        file_name=f"rekon_{port}_harian_kanal_{year_sel}_{month_sel:02d}.csv",
                        mime="text/csv"
                    )

                # 3) Rekap bulanan per kanal (tetap ada)
                met = build_metrics(port_df, h_col=h_col, aa_col=aa_col, amount_col=k_col)
                st.markdown(f"**Rekap Bulanan per Kanal — {port.title()}**")
                st.dataframe(df_format_id(met, cols=CHANNEL_COLS + ["Total"], decimals=0), use_container_width=True)
                st.download_button(
                    f"Unduh Rekap Bulanan {port.title()} (CSV)",
                    met.to_csv(index=False).encode('utf-8'),
                    file_name=f"rekon_ferizy_{port}_{year_sel}_{month_sel:02d}.csv",
                    mime="text/csv"
                )

    st.success("Selesai. Seksi Per Pelabuhan kini menampilkan data per tanggal (Total), dan rincian per kanal bisa ditampilkan bila diperlukan.")
except Exception as e:
    st.error("Terjadi error saat menjalankan aplikasi.")
    if error_detail:
        st.exception(e)
