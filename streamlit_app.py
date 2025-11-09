# app.py
import io
import pandas as pd
import numpy as np
import streamlit as st

st.set_page_config(page_title="Tabel Rekon Otomatis - Ferizy", layout="wide")

st.title("Detail Tiket from Payment Report Ferizy")
st.caption("Upload Payment Report (Excel/CSV), lalu sistem akan membuat tabel rekonsiliasi otomatis sesuai ketentuan.")

# -----------------------------
# Helpers
# -----------------------------
def resolve_column(df: pd.DataFrame, letter: str, pos_index: int, fallback_contains=None):
    """
    Resolve a column either by exact letter name (e.g., 'H'), by 0-based position,
    or (optionally) by a semantic name that contains a keyword.
    Returns tuple (col_name, found_by).
    """
    # 1) exact match (case-insensitive) e.g., 'H' / 'AA' / 'Q'
    for c in df.columns:
        if str(c).strip().lower() == letter.lower():
            return c, f"named '{letter}'"
    # 2) semantic contains
    if fallback_contains:
        for c in df.columns:
            if fallback_contains.lower() in str(c).strip().lower():
                return c, f"semantic match contains '{fallback_contains}'"
    # 3) by position
    if 0 <= pos_index < len(df.columns):
        return df.columns[pos_index], f"position index {pos_index} ({letter})"
    return None, "missing"

def normalize_str_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower()

@st.cache_data(show_spinner=False)
def read_any_file(uploaded_file, sheet=None):
    """
    Baca Excel/CSV dari Streamlit UploadedFile secara aman (pakai bytes buffer),
    dan kembalikan (df, sheets, chosen_sheet). Untuk CSV, sheets=None.
    """
    name = uploaded_file.name.lower()
    data = uploaded_file.getvalue()  # bytes

    if name.endswith(('.xlsx', '.xls')):
        xl = pd.ExcelFile(io.BytesIO(data))
        sheets = xl.sheet_names
        chosen = sheet if (sheet in sheets) else sheets[0]
        df = xl.parse(chosen, dtype=object)
        return df, sheets, chosen
    elif name.endswith('.csv'):
        df = pd.read_csv(io.BytesIO(data), dtype=object)
        return df, None, None
    else:
        st.error("Format tidak didukung. Unggah file Excel (.xlsx) atau CSV.")
        return None, None, None

def build_metrics(df, h_col, aa_col=None, amount_col=None):
    """
    Mengembalikan DataFrame satu baris berisi metrik kanal:
    - Jika amount_col None -> hitung jumlah baris.
    - Jika amount_col ada -> jumlahkan nilai numerik kolom tersebut.
    """
    if df.empty:
        return pd.DataFrame({
            'Cash':[0], 'Prepaid - BRI':[0], 'Prepaid - Mandiri':[0], 'Prepaid - BNI':[0],
            'Prepaid - BCA':[0], 'SKPT':[0], 'IFCS':[0], 'Redeem':[0], 'ESPAY':[0], 'FINNET':[0],
        })

    h_vals = normalize_str_series(df[h_col])

    def metric_for(mask):
        sub = df[mask] if (mask is not None and mask.any()) else df.iloc[0:0]
        if amount_col:
            vals = pd.to_numeric(sub[amount_col], errors='coerce')
            return float(vals.sum(skipna=True))
        else:
            return int(len(sub))

    # Base masks sesuai ketentuan
    cash_mask = h_vals.eq('cash')
    prepaid_bri_mask = h_vals.eq('prepaid-bri')
    prepaid_mandiri_mask = h_vals.eq('prepaid-mandiri')
    prepaid_bni_mask = h_vals.eq('prepaid-bni')
    prepaid_bca_mask = h_vals.eq('prepaid-bca')
    skpt_mask = h_vals.eq('skpt')
    ifcs_mask = h_vals.eq('cash')  # sesuai permintaan: IFCS ambil dari 'cash'
    redeem_mask = h_vals.eq('redeem')

    # ESPAY / FINNET require AA
    if aa_col is not None and aa_col in df.columns:
        aa_vals = normalize_str_series(df[aa_col])
        esp_mask = aa_vals.str.contains('esp', na=False)
        finnet_mask = ~aa_vals.str.contains('esp', na=False)
        espay_mask = h_vals.eq('finpay') & esp_mask
        finnet2_mask = h_vals.eq('finpay') & finnet_mask
    else:
        espay_mask = h_vals == '__no_matches__'    # False
        finnet2_mask = h_vals == '__no_matches__'  # False

    data = {
        'Cash': [metric_for(cash_mask)],
        'Prepaid - BRI': [metric_for(prepaid_bri_mask)],
        'Prepaid - Mandiri': [metric_for(prepaid_mandiri_mask)],
        'Prepaid - BNI': [metric_for(prepaid_bni_mask)],
        'Prepaid - BCA': [metric_for(prepaid_bca_mask)],
        'SKPT': [metric_for(skpt_mask)],
        'IFCS': [metric_for(ifcs_mask)],
        'Redeem': [metric_for(redeem_mask)],
        'ESPAY': [metric_for(espay_mask)],
        'FINNET': [metric_for(finnet2_mask)],
    }
    return pd.DataFrame(data)

def filter_port(df, q_col, port_name):
    q_vals = normalize_str_series(df[q_col])
    return df[q_vals.eq(port_name.strip().lower())]

# -----------------------------
# UI - Upload
# -----------------------------
uploaded = st.file_uploader("Upload Payment Report (Excel/CSV)", type=["xlsx", "xls", "csv"])

if not uploaded:
    st.info("Silakan upload file Payment Report untuk memulai.")
    st.stop()

# Baca file (dan pilih sheet jika Excel)
df, sheets, chosen_sheet = read_any_file(uploaded)
if df is None:
    st.stop()

if sheets:
    chosen_sheet = st.selectbox(
        "Pilih sheet: ",
        sheets,
        index=(sheets.index(chosen_sheet) if chosen_sheet in sheets else 0)
    )
    df, _, _ = read_any_file(uploaded, sheet=chosen_sheet)

st.write(":small_blue_diamond: Baris data:", len(df))

# -----------------------------
# Column resolution (H, AA, Q)
# -----------------------------
# Ekspektasi:
# H  -> channel/payment code (cash, prepaid-xxx, skpt, finpay, redeem, dll)
# AA -> description untuk deteksi 'ESP' (ESPAY vs FINNET)
# Q  -> Nama Pelabuhan
h_col, h_found = resolve_column(df, 'H', 7)
aa_col, aa_found = resolve_column(df, 'AA', 26)
q_col, q_found = resolve_column(df, 'Q', 16)

with st.expander("Lihat pemetaan kolom (opsional)"):
    st.write({
        "H": {"mapped_to": h_col, "how": h_found},
        "AA": {"mapped_to": aa_col, "how": aa_found},
        "Q": {"mapped_to": q_col, "how": q_found},
    })
    if h_col is None:
        st.error("Kolom H (channel) tidak ditemukan. Pastikan file mengikuti acuan: kolom H berisi cash/prepaid/finpay/... ")
    if aa_col is None:
        st.warning("Kolom AA (deskripsi) tidak ditemukan. ESPAY/FINNET akan bernilai 0.")
    if q_col is None:
        st.warning("Kolom Q (Nama Pelabuhan) tidak ditemukan. Tabel per pelabuhan tidak dapat dibuat.")

if h_col is None:
    st.stop()

# -----------------------------
# Opsi Agregasi
# -----------------------------
numeric_candidates = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
amount_mode = st.radio("Metode agregasi:", ["Hitung jumlah baris (default)", "Jumlahkan nilai kolom"], horizontal=True)
amount_col = None
if amount_mode == "Jumlahkan nilai kolom" and numeric_candidates:
    amount_col = st.selectbox("Pilih kolom nominal untuk dijumlahkan:", numeric_candidates)
elif amount_mode == "Jumlahkan nilai kolom" and not numeric_candidates:
    st.warning("Tidak ada kolom numerik terdeteksi. Menggunakan hitung baris.")

# -----------------------------
# Tabel utama - Semua Pelabuhan
# -----------------------------
st.subheader("Tabel Rekon Otomatis - Semua Pelabuhan")
main_metrics = build_metrics(df, h_col=h_col, aa_col=aa_col, amount_col=amount_col)
st.dataframe(main_metrics, use_container_width=True)

# Download CSV
main_csv = main_metrics.to_csv(index=False).encode('utf-8')
st.download_button("Unduh Rekon (CSV)", main_csv, file_name="rekon_ferizy_all.csv", mime="text/csv")

# -----------------------------
# Per Pelabuhan (Merak, Bakauheni, Ketapang)
# -----------------------------
if q_col is not None:
    st.subheader("Tabel Per Pelabuhan")
    tabs = st.tabs(["Merak", "Bakauheni", "Ketapang"])
    for tab, port in zip(tabs, ["merak", "bakauheni", "ketapang"]):
        with tab:
            port_df = filter_port(df, q_col, port)
            met = build_metrics(port_df, h_col=h_col, aa_col=aa_col, amount_col=amount_col)
            st.caption(f"Total baris: {len(port_df)}")
            st.dataframe(met, use_container_width=True)
            csv_bytes = met.to_csv(index=False).encode('utf-8')
            st.download_button(f"Unduh Rekon {port.title()} (CSV)", csv_bytes, file_name=f"rekon_ferizy_{port}.csv", mime="text/csv")

# -----------------------------
# Preview detail baris per channel
# -----------------------------
st.subheader("Preview Baris Detail per Channel (opsional)")
channel_choice = st.selectbox(
    "Pilih channel untuk preview:",
    ["Cash", "Prepaid - BRI", "Prepaid - Mandiri", "Prepaid - BNI", "Prepaid - BCA",
     "SKPT", "IFCS", "Redeem", "ESPAY", "FINNET"]
)

h_vals = normalize_str_series(df[h_col])
aa_vals = normalize_str_series(df[aa_col]) if (aa_col is not None and aa_col in df.columns) else pd.Series([None] * len(df))

mask_map = {
    "Cash": h_vals.eq('cash'),
    "Prepaid - BRI": h_vals.eq('prepaid-bri'),
    "Prepaid - Mandiri": h_vals.eq('prepaid-mandiri'),
    "Prepaid - BNI": h_vals.eq('prepaid-bni'),
    "Prepaid - BCA": h_vals.eq('prepaid-bca'),
    "SKPT": h_vals.eq('skpt'),
    "IFCS": h_vals.eq('cash'),
    "Redeem": h_vals.eq('redeem'),
    "ESPAY": (h_vals.eq('finpay') & aa_vals.str.contains('esp', na=False)) if (aa_col is not None and aa_col in df.columns) else (h_vals == '__no_matches__'),
    "FINNET": (h_vals.eq('finpay') & ~aa_vals.str.contains('esp', na=False)) if (aa_col is not None and aa_col in df.columns) else (h_vals == '__no_matches__'),
}

preview = df[mask_map[channel_choice]].head(200)
st.write(f"Menampilkan {len(preview)} baris (maks 200).")
st.dataframe(preview, use_container_width=True)

st.success("Selesai membuat Tabel Rekon Otomatis. Jika butuh format tambahan (pivot, grafik, atau ekspor Excel), beri tahu saya.")
