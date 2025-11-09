# app.py
import io
import pandas as pd
import numpy as np
import streamlit as st

st.set_page_config(page_title="Tabel Rekon Otomatis - Ferizy", layout="wide")

st.title("Detail Tiket from Payment Report Ferizy")
st.caption("Upload Payment Report (Excel/CSV). Aplikasi akan menambahkan kolom Tanggal (dari kolom B, tanpa jam) dan menjumlahkan nominal dari kolom K untuk setiap kanal.")

# -----------------------------
# Helpers
# -----------------------------
def resolve_column(df: pd.DataFrame, letter: str, pos_index: int, fallback_contains=None):
    """
    Cari kolom berdasarkan:
    1) Nama huruf persis (mis. 'H', 'AA', 'Q', 'B', 'K')
    2) (opsional) nama yang mengandung kata kunci
    3) Posisi 0-based (fallback)
    Return: (nama_kolom_ditemukan, cara_menemukan)
    """
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

@st.cache_data(show_spinner=False)
def read_any_file(uploaded_file, sheet=None):
    """
    Baca Excel/CSV dari Streamlit UploadedFile secara aman (bytes buffer),
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
    Kembalikan DataFrame satu baris berisi total nominal per kanal.
    - amount_col: kolom nominal yang dijumlahkan (kolom K). Jika None, fallback hitung baris.
    """
    if df.empty:
        return pd.DataFrame({
            'Cash':[0], 'Prepaid - BRI':[0], 'Prepaid - Mandiri':[0], 'Prepaid - BNI':[0],
            'Prepaid - BCA':[0], 'SKPT':[0], 'IFCS':[0], 'Redeem':[0], 'ESPAY':[0], 'FINNET':[0],
        })

    h_vals = normalize_str_series(df[h_col])

    def metric_for(mask):
        sub = df[mask] if (mask is not None and mask.any()) else df.iloc[0:0]
        if amount_col and amount_col in df.columns:
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
    ifcs_mask = h_vals.eq('cash')  # IFCS = ambil dari 'cash'
    redeem_mask = h_vals.eq('redeem')

    # ESPAY / FINNET perlu AA
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
# Upload
# -----------------------------
uploaded = st.file_uploader("Upload Payment Report (Excel/CSV)", type=["xlsx", "xls", "csv"])

if not uploaded:
    st.info("Silakan upload file Payment Report untuk memulai.")
    st.stop()

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
# Pemetaan kolom (B, H, K, AA, Q)
# -----------------------------
# B  -> Tanggal (ambil tanggal saja, abaikan jam)
# H  -> kanal/payment code (cash, prepaid-xxx, skpt, finpay, redeem, ...)
# K  -> amount/nominal
# AA -> deskripsi (untuk deteksi 'ESP' untuk ESPAY vs FINNET)
# Q  -> Nama Pelabuhan
b_col, b_found = resolve_column(df, 'B', 1)
h_col, h_found = resolve_column(df, 'H', 7)
k_col, k_found = resolve_column(df, 'K', 10)
aa_col, aa_found = resolve_column(df, 'AA', 26)
q_col, q_found = resolve_column(df, 'Q', 16)

# Buat kolom Tanggal dari B (tanggal saja)
if b_col is not None and b_col in df.columns:
    tanggal_parsed = pd.to_datetime(df[b_col], errors='coerce')
    df['Tanggal'] = tanggal_parsed.dt.date  # hanya tanggalnya
else:
    df['Tanggal'] = pd.NaT

with st.expander("Lihat pemetaan kolom (opsional)"):
    st.write({
        "B (Tanggal)": {"mapped_to": b_col, "how": b_found},
        "H (Kanal)": {"mapped_to": h_col, "how": h_found},
        "K (Amount)": {"mapped_to": k_col, "how": k_found},
        "AA (Deskripsi)": {"mapped_to": aa_col, "how": aa_found},
        "Q (Pelabuhan)": {"mapped_to": q_col, "how": q_found},
    })
    if h_col is None:
        st.error("Kolom H (kanal) tidak ditemukan. Pastikan kolom ini ada.")
    if k_col is None:
        st.warning("Kolom K (amount) tidak ditemukan. Akan fallback ke hitung baris, bukan jumlah nominal.")
    if b_col is None:
        st.warning("Kolom B (tanggal) tidak ditemukan. Kolom 'Tanggal' akan kosong.")
    if q_col is None:
        st.warning("Kolom Q (Nama Pelabuhan) tidak ditemukan. Tabel per pelabuhan tidak dapat dibuat.")

if h_col is None:
    st.stop()

# -----------------------------
# (Opsional) Filter tanggal
# -----------------------------
if df['Tanggal'].notna().any():
    min_d = pd.to_datetime(df['Tanggal']).min()
    max_d = pd.to_datetime(df['Tanggal']).max()
    d_range = st.date_input("Filter tanggal (opsional):", value=(min_d, max_d))
    if isinstance(d_range, tuple) and len(d_range) == 2:
        dmin, dmax = d_range
        mask_date = (pd.to_datetime(df['Tanggal']) >= pd.to_datetime(dmin)) & (pd.to_datetime(df['Tanggal']) <= pd.to_datetime(dmax))
        df = df[mask_date].copy()
        st.caption(f"Filter tanggal diterapkan: {dmin} s.d. {dmax}. Baris tersisa: {len(df)}")

# -----------------------------
# Tabel utama - Semua Pelabuhan (pakai K sebagai nominal)
# -----------------------------
st.subheader("Tabel Rekon Otomatis - Semua Pelabuhan (Jumlah Nominal dari Kolom K)")
main_metrics = build_metrics(df, h_col=h_col, aa_col=aa_col, amount_col=k_col)
st.dataframe(main_metrics, use_container_width=True)

# Unduh CSV
main_csv = main_metrics.to_csv(index=False).encode('utf-8')
st.download_button("Unduh Rekon (CSV)", main_csv, file_name="rekon_ferizy_all.csv", mime="text/csv")

# -----------------------------
# Per Pelabuhan (Merak, Bakauheni, Ketapang)
# -----------------------------
if q_col is not None:
    st.subheader("Tabel Per Pelabuhan (Nominal Kolom K)")
    tabs = st.tabs(["Merak", "Bakauheni", "Ketapang"])
    for tab, port in zip(tabs, ["merak", "bakauheni", "ketapang"]):
        with tab:
            port_df = filter_port(df, q_col, port)
            met = build_metrics(port_df, h_col=h_col, aa_col=aa_col, amount_col=k_col)
            st.caption(f"Total baris: {len(port_df)}")
            st.dataframe(met, use_container_width=True)
            csv_bytes = met.to_csv(index=False).encode('utf-8')
            st.download_button(f"Unduh Rekon {port.title()} (CSV)", csv_bytes, file_name=f"rekon_ferizy_{port}.csv", mime="text/csv")

# -----------------------------
# Preview detail baris per channel (tampilkan Tanggal & Amount)
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

preview_cols = ["Tanggal"] + [c for c in [h_col, k_col, aa_col, q_col] if c in df.columns]
preview = df[mask_map[channel_choice]].copy()
if not preview.empty:
    # Urutkan preview agar enak dibaca (tanggal terbaru di atas)
    if "Tanggal" in preview.columns:
        preview = preview.sort_values(by="Tanggal", ascending=False)
    # Tampilkan kolom penting dulu
    preview = preview[ [c for c in preview_cols if c in preview.columns] + [c for c in preview.columns if c not in preview_cols] ]

st.write(f"Menampilkan {len(preview)} baris (maks 200).")
st.dataframe(preview.head(200), use_container_width=True)

st.success("Selesai. Kolom 'Tanggal' (dari B) telah ditambahkan dan agregasi nominal memakai kolom 'K'.")
