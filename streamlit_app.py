# requirements.txt
streamlit>=1.33
pandas>=2.2
openpyxl>=3.1
xlsxwriter>=3.1

# app.py
# path: ./app.py
import io
from collections import OrderedDict
from typing import List, Optional

import pandas as pd
import streamlit as st


def _default_by_position(cols: List[str], pos: int) -> Optional[str]:
    """Return default column name by Excel-like position (0-based)."""
    return cols[pos] if len(cols) > pos else None


def _contains_token(series: pd.Series, token: str) -> pd.Series:
    token = (token or "").lower()
    return series.fillna("").astype(str).str.lower().str.contains(token, na=False)


def _startswith_token(series: pd.Series, prefix: str) -> pd.Series:
    prefix = (prefix or "").lower()
    return series.fillna("").astype(str).str.lower().str.startswith(prefix)


CATEGORY_RULES = OrderedDict(
    [
        ("Cash", lambda H, AA: _contains_token(H, "cash")),
        ("Prepaid BRI", lambda H, AA: _contains_token(H, "prepaid-bri")),
        ("Prepaid BNI", lambda H, AA: _contains_token(H, "prepaid-bni")),
        ("Prepaid Mandiri", lambda H, AA: _contains_token(H, "prepaid-mandiri")),
        ("Prepaid BCA", lambda H, AA: _contains_token(H, "prepaid-bca")),
        ("SKPT", lambda H, AA: _contains_token(H, "skpt")),
        ("IFCS", lambda H, AA: _contains_token(H, "ifcs")),
        ("Reedem", lambda H, AA: _contains_token(H, "reedem")),
        ("ESPAY", lambda H, AA: _contains_token(H, "finpay") & _startswith_token(AA, "esp")),
        ("Finnet", lambda H, AA: _contains_token(H, "finpay") & (~_startswith_token(AA, "esp"))),
    ]
)


def reconcile(
    df: pd.DataFrame,
    col_h: str,
    col_aa: Optional[str],
    amount_col: str,
    group_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Rekonsiliasi berdasarkan aturan kategori. Mengembalikan DataFrame kategori + Total."""
    if col_h not in df.columns:
        raise ValueError(f"Kolom H tidak ditemukan: {col_h}")
    if col_aa and col_aa not in df.columns:
        raise ValueError(f"Kolom AA tidak ditemukan: {col_aa}")
    if amount_col not in df.columns:
        raise ValueError(f"Kolom nominal tidak ditemukan: {amount_col}")

    H = df[col_h]
    AA = df[col_aa] if col_aa else pd.Series([""] * len(df), index=df.index)
    amount = pd.to_numeric(df[amount_col], errors="coerce").fillna(0)

    pieces = {}
    if group_cols:
        for name, rule in CATEGORY_RULES.items():
            mask = rule(H, AA)
            grp = df.loc[mask, group_cols].copy()
            grp["_amt"] = amount.loc[mask].values
            pieces[name] = grp.groupby(group_cols, dropna=False)["_amt"].sum(min_count=1)
        result = pd.concat(pieces, axis=1).fillna(0)
    else:
        idx = pd.Index(["TOTAL"])
        for name, rule in CATEGORY_RULES.items():
            mask = rule(H, AA)
            pieces[name] = pd.Series([amount.loc[mask].sum()], index=idx)
        result = pd.concat(pieces, axis=1).fillna(0)

    result["Total"] = result.sum(axis=1)
    return result


def main() -> None:
    st.set_page_config(page_title="Rekonsiliasi Payment Report", layout="wide")
    st.title("Rekonsiliasi Payment Report (Excel → Streamlit)")
    st.caption("Upload Excel, parse Tanggal dari kolom B (tanpa jam), amount dari kolom K, lalu rekonsiliasi kategori.")

    uploaded = st.file_uploader("Upload file Excel (.xlsx/.xls). CSV juga didukung.", type=["xlsx", "xls", "csv"])
    if not uploaded:
        st.info("Silakan upload file untuk mulai.")
        return

    # Load data
    if uploaded.name.lower().endswith((".xlsx", ".xls")):
        xls = pd.ExcelFile(uploaded)
        sheet = st.sidebar.selectbox("Pilih sheet", xls.sheet_names, index=0)
        df = xls.parse(sheet)
    else:
        df = pd.read_csv(uploaded)

    if df.empty:
        st.warning("Data kosong.")
        return

    cols = list(df.columns)

    # Default posisi Excel: B=1 (0-based), H=7, K=10, AA=26
    default_b = _default_by_position(cols, 1)
    default_h = _default_by_position(cols, 7)
    default_k = _default_by_position(cols, 10)
    default_aa = _default_by_position(cols, 26)

    st.sidebar.subheader("Pengaturan Kolom")
    # Kolom tanggal (B)
    try:
        idx_b = cols.index(default_b) if default_b in cols else 0
    except Exception:
        idx_b = 0
    col_b = st.sidebar.selectbox("Pilih kolom B (Tanggal, berisi tanggal+jam)", cols, index=idx_b)

    # Parse tanggal (abaikan jam) dan letakkan di kiri
    tanggal_series = pd.to_datetime(df[col_b], errors="coerce")
    # Kenapa: user minta abaikan jam → hanya tanggal
    tanggal_series = tanggal_series.dt.date
    df.insert(0, "Tanggal", tanggal_series)

    # Kolom H (kategori)
    try:
        idx_h = list(df.columns).index(default_h) if default_h in df.columns else 1
    except Exception:
        idx_h = 1
    col_h = st.sidebar.selectbox("Pilih kolom H (kategori)", list(df.columns), index=idx_h)

    # Kolom AA (opsional)
    aa_options = ["<tanpa kolom AA>"] + list(df.columns)
    idx_aa = 0
    if default_aa in df.columns:
        idx_aa = aa_options.index(default_aa)
    sel_aa = st.sidebar.selectbox("Pilih kolom AA (referensi)", aa_options, index=idx_aa)
    col_aa = None if sel_aa == "<tanpa kolom AA>" else sel_aa

    # Kolom K (amount)
    try:
        idx_k = list(df.columns).index(default_k) if default_k in df.columns else 1
    except Exception:
        idx_k = 1
    amount_col = st.sidebar.selectbox(
        "Pilih kolom K (Amount)", list(df.columns), index=idx_k,
        help="Sesuai permintaan: amount diambil dari kolom K secara default."
    )

    # Group by: default per Tanggal
    selectable_group_cols = [c for c in df.columns if c not in {amount_col}]
    default_groups = ["Tanggal"] if "Tanggal" in selectable_group_cols else []
    group_cols = st.sidebar.multiselect(
        "Group By (default per Tanggal)",
        options=selectable_group_cols,
        default=default_groups,
    )

    st.subheader("Preview Data (setelah tambah kolom Tanggal)")
    st.dataframe(df.head(20), use_container_width=True)

    with st.spinner("Menghitung rekonsiliasi..."):
        try:
            result = reconcile(
                df,
                col_h=col_h,
                col_aa=col_aa,
                amount_col=amount_col,  # Kenapa: user minta join amount dari K
                group_cols=group_cols or None,
            )
        except Exception as e:
            st.error(f"Gagal merekonsiliasi: {e}")
            return

    # Tampilkan hasil dengan Tanggal di paling kiri bila ada
    st.subheader("Hasil Rekonsiliasi")
    result_display = result.reset_index()
    if "Tanggal" in result_display.columns:
        # Pastikan Tanggal kolom paling kiri
        other_cols = [c for c in result_display.columns if c != "Tanggal"]
        result_display = result_display[["Tanggal"] + other_cols]
    st.dataframe(result_display, use_container_width=True)

    # Unduh hasil
    st.divider()
    st.subheader("Unduh Hasil")
    csv_bytes = result_display.to_csv(index=False).encode("utf-8-sig")
    st.download_button("Unduh CSV", data=csv_bytes, file_name="rekonsiliasi_payment.csv", mime="text/csv")

    xlsx_buf = io.BytesIO()
    with pd.ExcelWriter(xlsx_buf, engine="xlsxwriter") as writer:
        result_display.to_excel(writer, sheet_name="Rekonsiliasi", index=False)
    st.download_button(
        "Unduh Excel",
        data=xlsx_buf.getvalue(),
        file_name="rekonsiliasi_payment.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    with st.expander("Aturan & Catatan"):
        st.markdown(
            """
**Aturan Kategori**
- Cash → H mengandung `cash`
- Prepaid BRI → H `prepaid-bri`
- Prepaid BNI → H `prepaid-bni`
- Prepaid Mandiri → H `prepaid-mandiri`
- Prepaid BCA → H `prepaid-bca`
- SKPT → H `skpt`
- IFCS → H `ifcs`
- Reedem → H `reedem`
- ESPAY → H `finpay` **dan** AA diawali `esp`
- Finnet → H `finpay` **dan** AA **tidak** diawali `esp`
- **Total** = jumlah semua kategori.

**Catatan Implementasi**
- `Tanggal` diparse dari kolom B lalu diambil bagian harinya (jam diabaikan).
- Amount default dari kolom **K**, bisa diubah jika header/posisi berbeda.
- Default *group by* per `Tanggal`. Tambah kolom lain bila perlu.
"""
        )


if __name__ == "__main__":
    main()

# README.md
# Rekonsiliasi Payment Report (Streamlit)
Aplikasi Streamlit untuk rekonsiliasi payment report dari Excel.

## Fitur
- Upload Excel/CSV.
- Parse **Tanggal** dari **kolom B** (jam diabaikan), kolom ditempatkan paling kiri.
- Amount default dari **kolom K**.
- Kategori: Cash, Prepaid BRI/BNI/Mandiri/BCA, SKPT, IFCS, Reedem, ESPAY, Finnet.
- *Group by* default per **Tanggal**, dapat ditambah kolom lain.
- Ekspor CSV/XLSX.

## Jalankan Lokal
```bash
pip install -r requirements.txt
streamlit run app.py
