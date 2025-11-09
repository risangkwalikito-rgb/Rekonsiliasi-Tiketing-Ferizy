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
    "serta menyediakan parameter **bulan/tahun** agar **Tabel Harian** otomatis 1–28/29/30/31. "
    "Semua tabel kini memiliki kolom **Total**."
)

# =========================
# Helpers
# =========================
def resolve_column(df: pd.DataFrame, letter: str, pos_index: int, fallback_contains=None):
    """Cari kolom berdasarkan nama huruf (H/AA/Q/B/K), atau fallback posisi 0-based."""
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

def _read_csv_bytes(b: bytes) -> pd.DataFrame:
    """Baca CSV dari bytes secepat mungkin (pakai pyarrow jika ada)."""
    bio = io.BytesIO(b)
    try:
        # cepat dan hemat memori jika pyarrow tersedia
        return pd.read_csv(bio, dtype_backend="pyarrow", engine="pyarrow")
    except Exception:
        bio.seek(0)
        return pd.read_csv(bio, low_memory=False)

def _read_excel_bytes(b: bytes, sheet_name=None) -> pd.DataFrame:
    """Baca Excel dari bytes. Jika sheet_name None, ambil sheet pertama."""
    bio = io.BytesIO(b)
    xl = pd.ExcelFile(bio)
    target = sheet_name if (sheet_name in xl.sheet_names) else xl.sheet_names[0]
    return xl.parse(target, dtype=object)

@st.cache_data(show_spinner=False)
def read_single_file(uploaded_name: str, b: bytes, sheet=None):
    """Baca satu file (xlsx/xls/csv) dari bytes."""
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
    """
    Baca semua CSV + sheet pertama dari tiap Excel di dalam ZIP.
    Gabungkan menjadi satu DataFrame (union kolom).
    """
    zf = zipfile.ZipFile(io.BytesIO(archive_bytes))
    frames = []
    manifest = []
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
                # dilewati
                pass
        except Exception as e:
            manifest.append({"file": fname, "type": "unknown", "error": str(e)})

    if frames:
        # gabungkan kolom secara union
        df_all = pd.concat(frames, ignore_index=True, sort=False)
    else:
        df_all = pd.DataFrame()

    return df_all, manifest

def build_metrics(df, h_col, aa_col=None, amount_col=None):
    """
    Kembalikan DataFrame satu baris berisi total nominal per kanal (pakai K jika ada).
    Tambahkan kolom 'Total' (penjumlahan seluruh kanal).
    """
    cols_order = ["Cash", "Prepaid - BRI", "Prepaid - Mandiri", "Prepaid - BNI",
                  "Prepaid - BCA", "SKPT", "IFCS", "Redeem", "ESPAY", "FINNET"]
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
        espay_mask = h_vals.eq('finpay') & aa_vals.str.contains('esp', na=False)
        finnet_mask = h_vals.eq('finpay') & ~aa_vals.str.contains('esp', na=False)
        data['ESPAY'] = metric_for(espay_mask)
        data['FINNET'] = metric_for(finnet_mask)

    out = pd.DataFrame([data], columns=cols_order)
    out["Total"] = out[cols_order].sum(axis=1)
    return out

def build_daily_table(df_month, year_sel, month_sel, h_col, aa_col, amount_col, date_col='Tanggal'):
    """
    Tabel harian 1..N pada bulan year_sel/month_sel.
    Tiap hari = sum nominal (K) untuk tiap kanal + kolom Total.
    """
    last_day = calendar.monthrange(year_sel, month_sel)[1]
    all_days = pd.date_range(f"{year_sel}-{month_sel:02d}-01", periods=last_day, freq='D').date
    result = pd.DataFrame({"Tanggal": all_days})

    if df_month.empty:
        for c in ["Cash", "Prepaid - BRI", "Prepaid - Mandiri", "Prepaid - BNI",
                  "Prepaid - BCA", "SKPT", "IFCS", "Redeem", "ESPAY", "FINNET"]:
            result[c] = 0.0
        result["Total"] = 0.0
        return result

    # Series bantu
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
        "ESPAY": (h_vals.eq('finpay') & aa_vals.str.contains('esp', na=False)) if (aa_col is not None and aa_col in df_month.columns) else (h_vals == '__no_matches__'),
        "FINNET": (h_vals.eq('finpay') & ~aa_vals.str.contains('esp', na=False)) if (aa_col is not None and aa_col in df_month.columns) else (h_vals == '__no_matches__'),
    }

    for key, m in mask.items():
        s = pd.Series(np.where(m, amt, 0.0)).groupby(tgl).sum()
        s = s.reindex(all_days, fill_value=0.0)
        result[key] = s.values

    channel_cols = ["Cash", "Prepaid - BRI", "Prepaid - Mandiri", "Prepaid - BNI",
                    "Prepaid - BCA", "SKPT", "IFCS", "Redeem", "ESPAY", "FINNET"]
    result["Total"] = result[channel_cols].sum(axis=1)
    return result

def filter_port(df, q_col, port_name):
    q_vals = normalize_str_series(df[q_col])
    return df[q_vals.eq(port_name.strip().lower())]

# =========================
# SIDEBAR (kiri): Upload + Parameter
# =========================
with st.sidebar:
    st.header("📤 Upload & Parameter")
    uploaded = st.file_uploader(
        "Upload Payment Report (Excel/CSV/ZIP)",
        type=["xlsx", "xls", "csv", "zip"]
    )

    sheet_choice = None
    manifest_info = None

    if uploaded:
        name = uploaded.name.lower()
        data_bytes = uploaded.getvalue()

        if name.endswith(".zip"):
            df, manifest_info = read_zip(data_bytes)
            st.caption(f"ZIP terdeteksi. Tergabung: {len(df)} baris dari {len(manifest_info)} file.")
            with st.expander("Daftar isi ZIP"):
                st.write(manifest_info)
        else:
            # Single file
            df_tmp, sheets, chosen_sheet, mf = read_single_file(uploaded.name, data_bytes, sheet=None)
            manifest_info = mf
            if sheets:
                sheet_choice = st.selectbox("Pilih sheet:", sheets, index=sheets.index(chosen_sheet) if chosen_sheet in sheets else 0)
                df, sheets2, chosen2, _ = read_single_file(uploaded.name, data_bytes, sheet=sheet_choice)
            else:
                df = df_tmp
            st.caption(f"File: {uploaded.name} | Baris: {len(df)}")

    # Parameter Bulan/Tahun
    st.markdown("---")
    st.subheader("🗓️ Periode")
    if uploaded is not None and not df.empty:
        # Bikin kolom Tanggal dulu untuk dapat rentang default
        b_col, _ = resolve_column(df, 'B', 1)
        if b_col is not None and b_col in df.columns:
            tanggal_parsed = pd.to_datetime(df[b_col], errors='coerce')
            df['Tanggal'] = tanggal_parsed.dt.date
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
        years = [date.today().year]
        default_year = years[0]
        default_month = date.today().month

    bulan_id = ["Januari","Februari","Maret","April","Mei","Juni","Juli","Agustus","September","Oktober","November","Desember"]
    year_sel = st.selectbox("Tahun", years, index=years.index(default_year))
    month_sel_name = st.selectbox("Bulan", bulan_id, index=default_month-1)
    month_sel = bulan_id.index(month_sel_name) + 1

# =========================
# MAIN
# =========================
if not uploaded:
    st.info("Silakan upload file di sidebar kiri untuk memulai.")
    st.stop()

if df is None or df.empty:
    st.warning("Tidak ada data yang bisa dibaca dari file yang diunggah.")
    st.stop()

# Pemetaan kolom penting
h_col, h_found = resolve_column(df, 'H', 7)
k_col, k_found = resolve_column(df, 'K', 10)
aa_col, aa_found = resolve_column(df, 'AA', 26)
q_col, q_found = resolve_column(df, 'Q', 16)
# kolom Tanggal dari B (sudah dibuat di sidebar bagian upload)
b_col, b_found = resolve_column(df, 'B', 1)
if 'Tanggal' not in df.columns:
    if b_col is not None and b_col in df.columns:
        df['Tanggal'] = pd.to_datetime(df[b_col], errors='coerce').dt.date
    else:
        df['Tanggal'] = pd.NaT

with st.expander("Pemetaan kolom (opsional)"):
    st.write({
        "B (Tanggal)": {"mapped_to": b_col, "how": b_found},
        "H (Kanal)": {"mapped_to": h_col, "how": h_found},
        "K (Amount)": {"mapped_to": k_col, "how": k_found},
        "AA (Deskripsi)": {"mapped_to": aa_col, "how": aa_found},
        "Q (Pelabuhan)": {"mapped_to": q_col, "how": q_found},
    })
    if h_col is None:
        st.error("Kolom H (kanal) tidak ditemukan.")
    if k_col is None:
        st.warning("Kolom K (amount) tidak ditemukan. Tabel akan menghitung baris (bukan nominal).")
    if b_col is None:
        st.warning("Kolom B (tanggal) tidak ditemukan. Kolom 'Tanggal' mungkin kosong.")
    if q_col is None:
        st.warning("Kolom Q (Nama Pelabuhan) tidak ditemukan. Tabel per pelabuhan tidak dibuat.")

if h_col is None:
    st.stop()

# Filter ke bulan/tahun terpilih
if df['Tanggal'].notna().any():
    df_valid = df[df['Tanggal'].notna()].copy()
    df_valid['Tanggal_ts'] = pd.to_datetime(df_valid['Tanggal'])
    df_month = df_valid[
        (df_valid['Tanggal_ts'].dt.year == year_sel) &
        (df_valid['Tanggal_ts'].dt.month == month_sel)
    ].copy()
else:
    df_month = df.iloc[0:0].copy()

# Tabel Harian (selalu 1..N hari bulan tsb) + Total
st.subheader(f"Tabel Harian — {bulan_id[month_sel-1]} {year_sel} (sum kolom K) + Total")
daily_table = build_daily_table(
    df_month=df_month,
    year_sel=year_sel, month_sel=month_sel,
    h_col=h_col, aa_col=aa_col, amount_col=k_col, date_col='Tanggal'
)
st.dataframe(daily_table, use_container_width=True)
csv_daily = daily_table.to_csv(index=False).encode("utf-8")
st.download_button(
    "Unduh Tabel Harian (CSV)",
    csv_daily,
    file_name=f"rekon_harian_{year_sel}_{month_sel:02d}.csv",
    mime="text/csv"
)

# Rekap Bulanan (Semua Pelabuhan) + Total
st.subheader("Rekap Bulanan — Semua Pelabuhan (sum kolom K) + Total")
main_metrics_month = build_metrics(df_month, h_col=h_col, aa_col=aa_col, amount_col=k_col)
st.dataframe(main_metrics_month, use_container_width=True)
main_month_csv = main_metrics_month.to_csv(index=False).encode('utf-8')
st.download_button(
    "Unduh Rekap Bulanan (CSV)",
    main_month_csv,
    file_name=f"rekap_bulanan_{year_sel}_{month_sel:02d}.csv",
    mime="text/csv"
)

# Per Pelabuhan (Merak, Bakauheni, Ketapang) + Total
if q_col is not None and not df_month.empty:
    st.subheader("Tabel Per Pelabuhan (bulan terpilih) + Total")
    tabs = st.tabs(["Merak", "Bakauheni", "Ketapang"])
    for tab, port in zip(tabs, ["merak", "bakauheni", "ketapang"]):
        with tab:
            port_df = filter_port(df_month, q_col, port)
            met = build_metrics(port_df, h_col=h_col, aa_col=aa_col, amount_col=k_col)
            st.caption(f"Total baris {port.title()} (bulan ini): {len(port_df)}")
            st.dataframe(met, use_container_width=True)
            csv_bytes = met.to_csv(index=False).encode('utf-8')
            st.download_button(
                f"Unduh Rekon {port.title()} (CSV)",
                csv_bytes,
                file_name=f"rekon_ferizy_{port}_{year_sel}_{month_sel:02d}.csv",
                mime="text/csv"
            )

# Preview detail (bulan terpilih)
st.subheader("Preview Baris Detail per Channel (bulan terpilih)")
channel_choice = st.selectbox(
    "Pilih channel untuk preview:",
    ["Cash", "Prepaid - BRI", "Prepaid - Mandiri", "Prepaid - BNI", "Prepaid - BCA",
     "SKPT", "IFCS", "Redeem", "ESPAY", "FINNET"]
)
if not df_month.empty:
    h_vals = normalize_str_series(df_month[h_col])
    aa_vals = normalize_str_series(df_month[aa_col]) if (aa_col is not None and aa_col in df_month.columns) else pd.Series([None] * len(df_month))
    mask_map = {
        "Cash": h_vals.eq('cash'),
        "Prepaid - BRI": h_vals.eq('prepaid-bri'),
        "Prepaid - Mandiri": h_vals.eq('prepaid-mandiri'),
        "Prepaid - BNI": h_vals.eq('prepaid-bni'),
        "Prepaid - BCA": h_vals.eq('prepaid-bca'),
        "SKPT": h_vals.eq('skpt'),
        "IFCS": h_vals.eq('cash'),
        "Redeem": h_vals.eq('redeem'),
        "ESPAY": (h_vals.eq('finpay') & aa_vals.str.contains('esp', na=False)) if (aa_col is not None and aa_col in df_month.columns) else (h_vals == '__no_matches__'),
        "FINNET": (h_vals.eq('finpay') & ~aa_vals.str.contains('esp', na=False)) if (aa_col is not None and aa_col in df_month.columns) else (h_vals == '__no_matches__'),
    }
    preview_cols = ["Tanggal"] + [c for c in [h_col, k_col, aa_col, q_col] if c in df_month.columns]
    preview = df_month[mask_map[channel_choice]].copy()
    if not preview.empty:
        if "Tanggal" in preview.columns:
            preview = preview.sort_values(by="Tanggal", ascending=False)
        preview = preview[[c for c in preview_cols if c in preview.columns] + [c for c in preview.columns if c not in preview_cols]]
    st.write(f"Menampilkan {len(preview)} baris (maks 200).")
    st.dataframe(preview.head(200), use_container_width=True)
else:
    st.info("Tidak ada data pada bulan yang dipilih.")

st.success("Selesai. Upload ZIP didukung (otomatis gabung), pembacaan dipercepat, uploader di sidebar kiri, dan semua tabel punya kolom Total.")
