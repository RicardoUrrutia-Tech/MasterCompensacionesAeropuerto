import io
import re
import hashlib
from urllib.parse import urlparse

import pandas as pd
import requests
import streamlit as st


# =========================
# Config
# =========================
st.set_page_config(page_title="Máster Compensaciones Aeropuerto", layout="wide")

SALDO_SHEET_URL = "https://docs.google.com/spreadsheets/d/1Yj8q2dnlKqIZ1-vdr7wZZp_jvXiLoYO6Qwc8NeIeUnE/edit?gid=1139202449#gid=1139202449"
TRANSFER_SHEET_URL = "https://docs.google.com/spreadsheets/d/1yHTfTOD-N8VYBSzQRCkaNpMpAQHykBzVB5mYsXS6rHs/edit?resourcekey=&gid=1627777729#gid=1627777729"


# =========================
# Helpers
# =========================
def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def parse_sheet_id_and_gid(sheet_url: str) -> tuple[str, str]:
    """
    Extract spreadsheet id and gid from a Google Sheets URL.
    """
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
    if not m:
        raise ValueError("No se pudo extraer el spreadsheet_id desde el link.")
    spreadsheet_id = m.group(1)

    mgid = re.search(r"gid=([0-9]+)", sheet_url)
    if not mgid:
        raise ValueError("No se pudo extraer el gid desde el link.")
    gid = mgid.group(1)

    return spreadsheet_id, gid


def build_export_xlsx_url(sheet_url: str) -> str:
    spreadsheet_id, gid = parse_sheet_id_and_gid(sheet_url)
    # XLSX export by gid
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=xlsx&gid={gid}"


def fetch_bytes(url: str) -> bytes:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.content


def safe_read_table(file_bytes: bytes, filename_hint: str) -> pd.DataFrame:
    """
    Reads XLSX/CSV robustly from bytes.
    """
    name = (filename_hint or "").lower()
    bio = io.BytesIO(file_bytes)

    # Try excel first if looks like xlsx
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(bio)
    if name.endswith(".csv"):
        bio.seek(0)
        return pd.read_csv(bio)

    # Fallbacks
    try:
        bio.seek(0)
        return pd.read_excel(bio)
    except Exception:
        bio.seek(0)
        return pd.read_csv(bio)


def norm_str(x):
    if pd.isna(x):
        return ""
    return str(x).strip()


def extract_digits(s: str) -> str:
    return "".join(re.findall(r"\d+", s or ""))


def normalize_ticket(value) -> str:
    """
    Ticket can be:
    - 66171869
    - #66185638
    - https://cabify.zendesk.com/agent/tickets/67587605  -> 67587605 (after last /)
    """
    s = norm_str(value)
    if not s:
        return ""

    # If URL, take last path segment
    if "http://" in s or "https://" in s:
        try:
            p = urlparse(s)
            last = (p.path or "").rstrip("/").split("/")[-1]
            s = last
        except Exception:
            pass

    # Remove leading #
    s = s.replace("#", "").strip()
    # Keep digits only
    d = extract_digits(s)
    return d


def normalize_id_reserva(value) -> str:
    """
    Comes from 'Link payments, link del viaje o numero reserva'
    If URL -> last path segment, else digits.
    """
    s = norm_str(value)
    if not s:
        return ""

    if "http://" in s or "https://" in s:
        try:
            p = urlparse(s)
            last = (p.path or "").rstrip("/").split("/")[-1]
            # Could be uuid; keep as-is if has letters/hyphens, but also allow digits-only
            last = last.strip()
            if last:
                return last
        except Exception:
            pass

    # Not a URL: if looks like numeric reservation, keep digits
    d = extract_digits(s)
    return d if d else s


def parse_amount(value) -> float:
    """
    Heuristics to parse:
    - 19980
    - 14.968 -> 14968
    - 497.572 -> 497572
    - $28.47 (sometimes display quirk) -> try to interpret safely
    - 28.471 -> 28471
    """
    if pd.isna(value):
        return 0.0

    s = str(value).strip()
    if not s:
        return 0.0

    # Remove currency symbols and spaces
    s = s.replace("$", "").replace("CLP", "").replace(" ", "").replace("\u00a0", "")
    s = s.replace("\t", "")

    # If contains comma and dot, decide:
    # - "1.234,56" -> 1234.56 (EU)
    # - "1,234.56" -> 1234.56 (US)
    if "," in s and "." in s:
        # if last comma after last dot => comma is decimal
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")

    else:
        # If only comma: could be decimal or thousands; in CL usually thousands is dot
        # We'll treat comma as decimal only if it has 1-2 digits after it.
        if "," in s and "." not in s:
            parts = s.split(",")
            if len(parts) == 2 and len(parts[1]) in (1, 2):
                s = s.replace(",", ".")
            else:
                s = s.replace(",", "")

        # If only dot: in CL it's usually thousands separator; in some cases decimal.
        if "." in s and "," not in s:
            parts = s.split(".")
            # If pattern like 497.572 or 14.968 => thousands
            if all(p.isdigit() for p in parts) and len(parts[-1]) == 3:
                s = s.replace(".", "")
            # If pattern like 28.471 => also thousands-like (last group 3 digits)
            elif all(p.isdigit() for p in parts) and len(parts[-1]) == 3:
                s = s.replace(".", "")
            # If pattern like 28.47 (2 digits last), ambiguous:
            # we will interpret as decimal, but if it seems too small for compensation (<1000),
            # we also try removing dot and choose the larger plausible integer.
            elif all(p.isdigit() for p in parts) and len(parts[-1]) == 2:
                try_decimal = None
                try_int = None
                try:
                    try_decimal = float(s)
                except Exception:
                    try_decimal = None
                try:
                    try_int = float(s.replace(".", ""))
                except Exception:
                    try_int = None

                # Choose:
                # - if decimal is < 1000 and integer is >= 1000, prefer integer (compensaciones suelen ser miles)
                if try_decimal is not None and try_int is not None:
                    if try_decimal < 1000 and try_int >= 1000:
                        s = s.replace(".", "")
                # else keep as is and parse float

    # Remove any stray non-numeric (except dot and minus)
    s = re.sub(r"[^0-9\.\-]", "", s)
    if not s or s == "." or s == "-" or s == "-.":
        return 0.0

    try:
        return float(s)
    except Exception:
        # last resort: keep digits only
        d = extract_digits(s)
        return float(d) if d else 0.0


def to_datetime_series(col: pd.Series) -> pd.Series:
    # Try dayfirst first (Chile), then fallback
    dt = pd.to_datetime(col, errors="coerce", dayfirst=True)
    if dt.isna().mean() > 0.5:
        dt2 = pd.to_datetime(col, errors="coerce")
        # use dt2 where dt missing
        dt = dt.fillna(dt2)
    return dt


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    df2.columns = [str(c).strip() for c in df2.columns]
    return df2


# =========================
# Transformations
# =========================
def build_master(df_saldo_raw: pd.DataFrame, df_transfer_raw: pd.DataFrame) -> pd.DataFrame:
    df_saldo_raw = normalize_columns(df_saldo_raw)
    df_transfer_raw = normalize_columns(df_transfer_raw)

    # --- SALDO base ---
    # Expected columns (user described):
    # Marca temporal | Dirección de correo electrónico | Numero ticket | Correo registrado... | Monto a compensar | Motivo compensación
    # Sometimes first column is date-time without header; handle lightly.
    col_candidates = {c.lower(): c for c in df_saldo_raw.columns}

    # Try to detect date column
    date_col = None
    for c in df_saldo_raw.columns:
        if "marca" in c.lower() and "temporal" in c.lower():
            date_col = c
            break
    if date_col is None:
        # fallback: first column
        date_col = df_saldo_raw.columns[0]

    email_col = None
    for c in df_saldo_raw.columns:
        if "dirección" in c.lower() and "correo" in c.lower():
            email_col = c
            break
    if email_col is None:
        # fallback: any column containing 'correo' and 'electr'
        for c in df_saldo_raw.columns:
            if "correo" in c.lower():
                email_col = c
                break

    ticket_col = None
    for c in df_saldo_raw.columns:
        if "ticket" in c.lower():
            ticket_col = c
            break

    correo_carga_col = None
    for c in df_saldo_raw.columns:
        if "cabify" in c.lower() and "carga" in c.lower():
            correo_carga_col = c
            break
    if correo_carga_col is None:
        # heuristic: a column that contains 'carga'
        for c in df_saldo_raw.columns:
            if "carga" in c.lower():
                correo_carga_col = c
                break

    monto_col = None
    for c in df_saldo_raw.columns:
        if "monto" in c.lower():
            monto_col = c
            break

    motivo_col = None
    for c in df_saldo_raw.columns:
        if "motivo" in c.lower():
            motivo_col = c
            break

    df_saldo = pd.DataFrame({
        "Fecha": to_datetime_series(df_saldo_raw[date_col]) if date_col in df_saldo_raw else pd.NaT,
        "Dirección de correo electrónico": df_saldo_raw[email_col] if email_col in df_saldo_raw else "",
        "Numero": df_saldo_raw[ticket_col] if ticket_col in df_saldo_raw else "",
        "Correo registrado en Cabify para realizar la carga": df_saldo_raw[correo_carga_col] if correo_carga_col in df_saldo_raw else "",
        "Monto_saldo": df_saldo_raw[monto_col] if monto_col in df_saldo_raw else 0,
        "Motivo_saldo": df_saldo_raw[motivo_col] if motivo_col in df_saldo_raw else "",
    })

    df_saldo["Numero"] = df_saldo["Numero"].apply(normalize_ticket)
    df_saldo["Monto_saldo"] = df_saldo["Monto_saldo"].apply(parse_amount)
    df_saldo["Motivo_saldo"] = df_saldo["Motivo_saldo"].apply(norm_str)
    df_saldo["Dirección de correo electrónico"] = df_saldo["Dirección de correo electrónico"].apply(norm_str)
    df_saldo["Correo registrado en Cabify para realizar la carga"] = df_saldo["Correo registrado en Cabify para realizar la carga"].apply(norm_str)
    df_saldo["Clasificación_saldo"] = "Aeropuerto - Saldo"

    # Drop rows without ticket
    df_saldo = df_saldo[df_saldo["Numero"].astype(str).str.len() > 0].copy()

    # --- TRANSFER base ---
    # Must filter Motivo == "Compensación Aeropuerto"
    motivo_main_col = None
    for c in df_transfer_raw.columns:
        if c.strip().lower() == "motivo":
            motivo_main_col = c
            break
    if motivo_main_col is None:
        # fallback: any column named like 'Motivo '
        for c in df_transfer_raw.columns:
            if c.lower().strip() == "motivo":
                motivo_main_col = c
                break

    if motivo_main_col is None:
        raise ValueError("No encontré la columna 'Motivo' en la base de Transferencias.")

    df_transfer_f = df_transfer_raw[df_transfer_raw[motivo_main_col].astype(str).str.strip().eq("Compensación Aeropuerto")].copy()

    # Identify columns we need:
    # Marca temporal (Fecha), Dirección de correo electrónico, Ticket, "Si es compensación..." (motivo aeropuerto), Link payments/link viaje/num reserva (id_reserva), Monto (el monto asociado a la compensación)
    date2_col = None
    for c in df_transfer_f.columns:
        if "marca" in c.lower() and "temporal" in c.lower():
            date2_col = c
            break
    if date2_col is None:
        date2_col = df_transfer_f.columns[0]

    email2_col = None
    for c in df_transfer_f.columns:
        if "dirección" in c.lower() and "correo" in c.lower():
            email2_col = c
            break

    ticket2_col = None
    for c in df_transfer_f.columns:
        if c.lower().strip() == "ticket":
            ticket2_col = c
            break
    if ticket2_col is None:
        for c in df_transfer_f.columns:
            if "ticket" in c.lower():
                ticket2_col = c
                break

    motivo_air_col = None
    for c in df_transfer_f.columns:
        if "si es compensación aeropuerto" in c.lower():
            motivo_air_col = c
            break
    if motivo_air_col is None:
        # fallback: might have slightly different header
        for c in df_transfer_f.columns:
            if "compensación aeropuerto" in c.lower() and "selecciona" in c.lower():
                motivo_air_col = c
                break

    id_reserva_col = None
    for c in df_transfer_f.columns:
        if "link payments" in c.lower() and "numero reserva" in c.lower():
            id_reserva_col = c
            break
    if id_reserva_col is None:
        for c in df_transfer_f.columns:
            if "numero reserva" in c.lower() or "link del viaje" in c.lower():
                id_reserva_col = c
                break

    # Amount column: there are multiple "Monto" columns in that sheet.
    # We'll choose the one that appears AFTER "Link payments, link del viaje o numero reserva" if possible,
    # otherwise the last column named exactly "Monto".
    monto_cols = [c for c in df_transfer_f.columns if c.lower().strip() == "monto"]
    monto2_col = None
    if id_reserva_col and monto_cols:
        # pick the first "Monto" that occurs after id_reserva_col index, else last "Monto"
        cols = list(df_transfer_f.columns)
        idx_id = cols.index(id_reserva_col) if id_reserva_col in cols else -1
        after = [c for c in monto_cols if cols.index(c) > idx_id]
        monto2_col = after[0] if after else monto_cols[-1]
    elif monto_cols:
        monto2_col = monto_cols[-1]
    else:
        # fallback: any column containing "monto"
        for c in reversed(df_transfer_f.columns):
            if "monto" in c.lower():
                monto2_col = c
                break

    df_transfer = pd.DataFrame({
        "Fecha": to_datetime_series(df_transfer_f[date2_col]) if date2_col in df_transfer_f else pd.NaT,
        "Dirección de correo electrónico": df_transfer_f[email2_col] if email2_col in df_transfer_f else "",
        "Numero": df_transfer_f[ticket2_col] if ticket2_col in df_transfer_f else "",
        "Correo registrado en Cabify para realizar la carga": df_transfer_f.get("Correo", ""),  # in transfer sheet it's "Correo"
        "Monto_transferencia": df_transfer_f[monto2_col] if monto2_col in df_transfer_f else 0,
        "Motivo_transferencia": df_transfer_f[motivo_air_col] if motivo_air_col in df_transfer_f else "",
        "id_reserva": df_transfer_f[id_reserva_col] if id_reserva_col in df_transfer_f else "",
    })

    df_transfer["Numero"] = df_transfer["Numero"].apply(normalize_ticket)
    df_transfer["Monto_transferencia"] = df_transfer["Monto_transferencia"].apply(parse_amount)
    df_transfer["Motivo_transferencia"] = df_transfer["Motivo_transferencia"].apply(norm_str)
    df_transfer["Dirección de correo electrónico"] = df_transfer["Dirección de correo electrónico"].apply(norm_str)
    df_transfer["Correo registrado en Cabify para realizar la carga"] = df_transfer["Correo registrado en Cabify para realizar la carga"].apply(norm_str)
    df_transfer["id_reserva"] = df_transfer["id_reserva"].apply(normalize_id_reserva)
    df_transfer["Clasificación_transferencia"] = "Aeropuerto - Transferencia"

    df_transfer = df_transfer[df_transfer["Numero"].astype(str).str.len() > 0].copy()

    # --- Combine / Master ---
    # Prepare keys
    df_saldo_k = df_saldo.copy()
    df_transfer_k = df_transfer.copy()

    # Merge outer on ticket
    master = df_saldo_k.merge(
        df_transfer_k,
        on="Numero",
        how="outer",
        suffixes=("_saldo_side", "_transfer_side")
    )

    # Consolidate fields
    master["Fecha"] = master["Fecha_saldo_side"].combine_first(master["Fecha_transfer_side"])

    # Prefer transfer email if saldo missing, else saldo
    master["Dirección de correo electrónico"] = master["Dirección de correo electrónico_saldo_side"].where(
        master["Dirección de correo electrónico_saldo_side"].astype(str).str.len() > 0,
        master["Dirección de correo electrónico_transfer_side"]
    )

    # Correo carga: prefer saldo one, else transfer one
    master["Correo registrado en Cabify para realizar la carga"] = master["Correo registrado en Cabify para realizar la carga_saldo_side"].where(
        master["Correo registrado en Cabify para realizar la carga_saldo_side"].astype(str).str.len() > 0,
        master["Correo registrado en Cabify para realizar la carga_transfer_side"]
    )

    # Amount sum
    master["Monto a compensar"] = master["Monto_saldo"].fillna(0) + master["Monto_transferencia"].fillna(0)

    # Motivo unified: prefer non-empty; if both, join unique
    def combine_motivos(row):
        m1 = norm_str(row.get("Motivo_saldo"))
        m2 = norm_str(row.get("Motivo_transferencia"))
        vals = [v for v in [m1, m2] if v]
        # unique preserving order
        out = []
        for v in vals:
            if v not in out:
                out.append(v)
        return " | ".join(out)

    master["Motivo compensación"] = master.apply(combine_motivos, axis=1)

    # id_reserva from transfer side if present
    master["id_reserva"] = master.get("id_reserva", "")

    # Clasificación
    def classify(row):
        has_saldo = pd.notna(row.get("Monto_saldo")) and float(row.get("Monto_saldo") or 0) > 0
        has_trans = pd.notna(row.get("Monto_transferencia")) and float(row.get("Monto_transferencia") or 0) > 0
        if has_saldo and has_trans:
            return "Aeropuerto - Mixta (Saldo + Transferencia)"
        if has_saldo:
            return "Aeropuerto - Saldo"
        if has_trans:
            return "Aeropuerto - Transferencia"
        # edge case: ticket exists but amounts empty
        return "Aeropuerto - Sin monto"

    master["Clasificación"] = master.apply(classify, axis=1)

    # Output columns required
    out = master[[
        "Fecha",
        "Dirección de correo electrónico",
        "Numero",
        "Correo registrado en Cabify para realizar la carga",
        "Monto a compensar",
        "Motivo compensación",
        "id_reserva",
        "Clasificación",
    ]].copy()

    # Final cleanup
    out["Fecha"] = pd.to_datetime(out["Fecha"], errors="coerce")
    out = out.sort_values(["Fecha", "Numero"], na_position="last").reset_index(drop=True)

    return out


# =========================
# UI
# =========================
st.title("Máster de Compensaciones Aeropuerto (Saldo + Transferencias)")

with st.expander("✅ Qué hace esta app", expanded=False):
    st.markdown(
        """
- Descarga los 2 reportes desde Google Sheets (botones) y/o permite subirlos localmente.
- Valida si el archivo local **coincide** con el Sheet (checksum SHA-256).
- Normaliza Ticket (número / # / URL), Monto y extrae `id_reserva` desde el campo de la base de transferencias.
- Filtra transferencias por `Motivo = Compensación Aeropuerto` y unifica el motivo aeropuerto con el motivo de Saldo.
- Genera un **máster 1 registro por ticket** sumando montos si aparece en ambas fuentes.
        """
    )

colA, colB = st.columns(2, gap="large")

# ---- SALDO block ----
with colA:
    st.subheader("1) Reporte Carga de Saldo (Aeropuerto)")

    saldo_export_url = build_export_xlsx_url(SALDO_SHEET_URL)
    st.caption("Fuente Google Sheet")
    st.code(SALDO_SHEET_URL, language="text")

    saldo_fetch = st.button("⬇️ Descargar desde Sheet (Saldo)", use_container_width=True)

    if "saldo_bytes" not in st.session_state:
        st.session_state["saldo_bytes"] = None
        st.session_state["saldo_sha"] = None
        st.session_state["saldo_fetch_error"] = None

    if saldo_fetch:
        try:
            b = fetch_bytes(saldo_export_url)
            st.session_state["saldo_bytes"] = b
            st.session_state["saldo_sha"] = sha256_bytes(b)
            st.session_state["saldo_fetch_error"] = None
        except requests.exceptions.HTTPError as e:
            st.session_state["saldo_fetch_error"] = f"HTTPError al descargar Saldo: {e}"
        except Exception as e:
            st.session_state["saldo_fetch_error"] = f"Error al descargar Saldo: {e}"

    if st.session_state["saldo_fetch_error"]:
        st.error(st.session_state["saldo_fetch_error"])
        st.info(
            "Revisa que el Google Sheet esté compartido como **'Cualquiera con el enlace'** (lector). "
            "Si no, Streamlit Cloud no podrá descargarlo sin autenticación."
        )

    if st.session_state["saldo_bytes"]:
        st.download_button(
            "💾 Guardar XLSX descargado (Saldo)",
            data=st.session_state["saldo_bytes"],
            file_name="saldo_compensaciones.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
        st.caption(f"Checksum (SHA-256) Sheet: `{st.session_state['saldo_sha']}`")

    saldo_up = st.file_uploader(
        "📤 O carga el archivo local (Saldo) (XLSX/CSV)",
        type=["xlsx", "xls", "csv"],
        key="saldo_uploader"
    )

# ---- TRANSFER block ----
with colB:
    st.subheader("2) Reporte Compensaciones Transferencia (filtra Aeropuerto)")

    transfer_export_url = build_export_xlsx_url(TRANSFER_SHEET_URL)
    st.caption("Fuente Google Sheet")
    st.code(TRANSFER_SHEET_URL, language="text")

    transfer_fetch = st.button("⬇️ Descargar desde Sheet (Transferencias)", use_container_width=True)

    if "transfer_bytes" not in st.session_state:
        st.session_state["transfer_bytes"] = None
        st.session_state["transfer_sha"] = None
        st.session_state["transfer_fetch_error"] = None

    if transfer_fetch:
        try:
            b = fetch_bytes(transfer_export_url)
            st.session_state["transfer_bytes"] = b
            st.session_state["transfer_sha"] = sha256_bytes(b)
            st.session_state["transfer_fetch_error"] = None
        except requests.exceptions.HTTPError as e:
            st.session_state["transfer_fetch_error"] = f"HTTPError al descargar Transferencias: {e}"
        except Exception as e:
            st.session_state["transfer_fetch_error"] = f"Error al descargar Transferencias: {e}"

    if st.session_state["transfer_fetch_error"]:
        st.error(st.session_state["transfer_fetch_error"])
        st.info(
            "Revisa que el Google Sheet esté compartido como **'Cualquiera con el enlace'** (lector). "
            "Si no, Streamlit Cloud no podrá descargarlo sin autenticación."
        )

    if st.session_state["transfer_bytes"]:
        st.download_button(
            "💾 Guardar XLSX descargado (Transferencias)",
            data=st.session_state["transfer_bytes"],
            file_name="transfer_compensaciones.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
        st.caption(f"Checksum (SHA-256) Sheet: `{st.session_state['transfer_sha']}`")

    transfer_up = st.file_uploader(
        "📤 O carga el archivo local (Transferencias) (XLSX/CSV)",
        type=["xlsx", "xls", "csv"],
        key="transfer_uploader"
    )

st.divider()

# =========================
# Load selected sources + checksum verification
# =========================
def choose_bytes(uploaded_file, session_bytes):
    if uploaded_file is not None:
        return uploaded_file.getvalue(), uploaded_file.name, "local"
    if session_bytes is not None:
        return session_bytes, "downloaded_from_sheet.xlsx", "sheet"
    return None, "", "none"


saldo_bytes, saldo_name, saldo_origin = choose_bytes(saldo_up, st.session_state["saldo_bytes"])
transfer_bytes, transfer_name, transfer_origin = choose_bytes(transfer_up, st.session_state["transfer_bytes"])

colC, colD = st.columns(2, gap="large")
with colC:
    st.subheader("Validación Saldo")
    if saldo_bytes is None:
        st.warning("Aún no hay archivo de Saldo (descarga o carga local).")
    else:
        local_sha = sha256_bytes(saldo_bytes)
        st.write(f"Origen: **{saldo_origin}** — archivo: `{saldo_name}`")
        st.caption(f"Checksum archivo actual: `{local_sha}`")
        if st.session_state["saldo_sha"]:
            if local_sha != st.session_state["saldo_sha"] and saldo_origin == "local":
                st.error("El archivo local **NO coincide** con lo descargado desde el Sheet (checksum distinto).")
                st.caption("Esto puede ser normal si el Sheet cambió o si el archivo es otra versión.")

with colD:
    st.subheader("Validación Transferencias")
    if transfer_bytes is None:
        st.warning("Aún no hay archivo de Transferencias (descarga o carga local).")
    else:
        local_sha = sha256_bytes(transfer_bytes)
        st.write(f"Origen: **{transfer_origin}** — archivo: `{transfer_name}`")
        st.caption(f"Checksum archivo actual: `{local_sha}`")
        if st.session_state["transfer_sha"]:
            if local_sha != st.session_state["transfer_sha"] and transfer_origin == "local":
                st.error("El archivo local **NO coincide** con lo descargado desde el Sheet (checksum distinto).")
                st.caption("Esto puede ser normal si el Sheet cambió o si el archivo es otra versión.")

st.divider()

# =========================
# Build master
# =========================
st.subheader("3) Generar Máster (1 registro por ticket)")

generate = st.button("⚙️ Generar Máster", type="primary", use_container_width=True)

if generate:
    if saldo_bytes is None or transfer_bytes is None:
        st.error("Falta al menos uno de los archivos (Saldo y/o Transferencias). Descárgalos o súbelos localmente.")
    else:
        try:
            df_saldo_raw = safe_read_table(saldo_bytes, saldo_name)
            df_transfer_raw = safe_read_table(transfer_bytes, transfer_name)

            master = build_master(df_saldo_raw, df_transfer_raw)

            st.success(f"Listo: {len(master):,} tickets únicos en el máster.".replace(",", "."))

            st.dataframe(master, use_container_width=True, height=420)

            # Download result
            out_bio = io.BytesIO()
            with pd.ExcelWriter(out_bio, engine="openpyxl") as writer:
                master.to_excel(writer, index=False, sheet_name="master")
            out_bio.seek(0)

            st.download_button(
                "⬇️ Descargar Máster (XLSX)",
                data=out_bio.getvalue(),
                file_name="master_compensaciones_aeropuerto.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

        except Exception as e:
            st.exception(e)

st.caption(
    "Nota: Si vuelve a aparecer HTTPError al descargar, casi siempre es por permisos del Sheet. "
    "Solución: compartir como 'Cualquiera con el enlace' (lector)."
)


