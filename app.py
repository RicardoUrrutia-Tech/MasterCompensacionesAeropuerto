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


def parse_sheet_id_gid(sheet_url: str):
    """
    Extract spreadsheet_id and gid from a Google Sheets URL.
    """
    m = re.search(r"/spreadsheets/d/([^/]+)/", sheet_url)
    if not m:
        return None, None
    spreadsheet_id = m.group(1)
    gid_m = re.search(r"gid=(\d+)", sheet_url)
    gid = gid_m.group(1) if gid_m else None
    return spreadsheet_id, gid


def export_xlsx_url(sheet_url: str) -> str:
    """
    Unauthenticated export endpoint.
    NOTE: Will fail (403/404) if the sheet is not accessible publicly from the server.
    """
    sid, gid = parse_sheet_id_gid(sheet_url)
    if not sid or not gid:
        return sheet_url
    return f"https://docs.google.com/spreadsheets/d/{sid}/export?format=xlsx&gid={gid}"


def fetch_bytes(url: str, timeout: int = 30) -> bytes:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; MasterCompensaciones/1.0; +https://streamlit.io)"
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.content


def read_excel_any(b: bytes) -> pd.DataFrame:
    """
    Read first sheet of an xlsx into DataFrame.
    """
    bio = io.BytesIO(b)
    return pd.read_excel(bio, engine="openpyxl")


def normalize_colnames(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def pick_first_col(df: pd.DataFrame, candidates):
    """
    Return the first existing column name from candidates. If duplicates exist,
    pandas might expose df[col] as a DataFrame (2D). We always take the first.
    """
    for c in candidates:
        if c in df.columns:
            # Handle duplicate column names
            sel = df.loc[:, c]
            if isinstance(sel, pd.DataFrame):
                return sel.columns[0]  # first duplicate
            return c
    return None


def to_datetime_series(s: pd.Series) -> pd.Series:
    """
    Robust datetime parsing for dd/mm/yyyy and timestamps.
    """
    if s is None:
        return pd.Series([pd.NaT] * 0)
    # Ensure 1D
    if isinstance(s, pd.DataFrame):
        s = s.iloc[:, 0]
    s2 = s.copy()
    # Strings
    s2 = s2.astype(str).str.strip()
    s2 = s2.replace({"nan": "", "None": ""})
    dt = pd.to_datetime(s2, errors="coerce", dayfirst=True)
    return dt


def extract_ticket(value) -> str:
    """
    Ticket can be:
      - '66171869'
      - '#66185638'
      - 'https://cabify.zendesk.com/agent/tickets/67587605' -> 67587605
    We return digits as string, or ''.
    """
    if pd.isna(value):
        return ""
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none"):
        return ""

    # If URL, take last path segment
    if "http://" in s or "https://" in s:
        try:
            path = urlparse(s).path.strip("/")
            last = path.split("/")[-1] if path else s
            s = last
        except Exception:
            pass

    # Remove leading '#'
    s = s.lstrip("#").strip()

    # Extract last run of digits (prefer long)
    m = re.findall(r"\d+", s)
    if not m:
        return ""
    # choose the longest chunk, if tie choose last
    m_sorted = sorted(m, key=lambda x: (len(x), m.index(x)))
    ticket = m_sorted[-1]
    return ticket


def extract_id_reserva(value) -> str:
    """
    Field: 'Link payments, link del viaje o numero reserva'
    If URL, keep last path segment; else keep trimmed text.
    """
    if pd.isna(value):
        return ""
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none"):
        return ""
    if "http://" in s or "https://" in s:
        try:
            path = urlparse(s).path.strip("/")
            last = path.split("/")[-1] if path else s
            return last.strip()
        except Exception:
            return s
    return s


def parse_clp_amount(x) -> float:
    """
    Parse amounts that may look like:
      - 19980
      - 7000
      - 13.990  (thousands dot)
      - 497.572 (thousands dot)
      - $28.47  (ambiguous; we parse as 28.47 -> 28.47)
      - 28.471  (thousands dot pattern -> 28471)
    Return float (later we can round to int if desired).
    """
    if pd.isna(x):
        return 0.0

    # If numeric already
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        try:
            if pd.isna(x):
                return 0.0
        except Exception:
            pass
        return float(x)

    s = str(x).strip()
    if not s or s.lower() in ("nan", "none"):
        return 0.0

    # Remove currency symbols and spaces
    s = s.replace("$", "").replace("CLP", "").replace(" ", "").strip()

    # If pattern like 497.572 or 14.968 or 28.471 -> treat dot as thousands
    if re.fullmatch(r"\d{1,3}(\.\d{3})+", s):
        return float(s.replace(".", ""))

    # If it contains comma as decimal separator (rare here), normalize
    # Example: 10.990,50 -> 10990.50 or 10,5 -> 10.5
    if "," in s and "." in s:
        # assume dot thousands, comma decimal
        s = s.replace(".", "").replace(",", ".")
        try:
            return float(s)
        except Exception:
            return 0.0
    if "," in s and "." not in s:
        # assume comma decimal
        try:
            return float(s.replace(",", "."))
        except Exception:
            return 0.0

    # Plain float/int with dot decimal
    try:
        return float(s)
    except Exception:
        # last attempt: keep digits only
        digs = re.findall(r"\d+", s)
        return float(digs[0]) if digs else 0.0


def safe_1d(series_or_df, length: int):
    """
    Ensure we return a 1D Series of required length, filling missing with ''.
    """
    if series_or_df is None:
        return pd.Series([""] * length)
    if isinstance(series_or_df, pd.DataFrame):
        s = series_or_df.iloc[:, 0]
    else:
        s = series_or_df
    if len(s) != length:
        # reindex to length
        s = pd.Series(list(s) + [""] * max(0, length - len(s)))
        s = s.iloc[:length]
    return s


# =========================
# Master builder
# =========================
def build_master(df_saldo_raw: pd.DataFrame, df_transfer_raw: pd.DataFrame) -> pd.DataFrame:
    df_saldo_raw = normalize_colnames(df_saldo_raw)
    df_transfer_raw = normalize_colnames(df_transfer_raw)

    # --- SALDO (compensaciones con cargo de saldo) ---
    # Expected columns:
    # Marca temporal (or similar), Dirección de correo electrónico, Numero ticket, Correo registrado..., Monto..., Motivo compensación
    saldo_date_col = pick_first_col(df_saldo_raw, ["Marca temporal", "Fecha", "Timestamp", "marca temporal"])
    saldo_email_col = pick_first_col(df_saldo_raw, ["Dirección de correo electrónico", "Direccion de correo electronico", "Email", "Correo", "Dirección correo electrónico"])
    saldo_ticket_col = pick_first_col(df_saldo_raw, ["Numero ticket", "Número ticket", "Ticket", "Numero", "N° ticket"])
    saldo_cabify_mail_col = pick_first_col(df_saldo_raw, ["Correo registrado en Cabify para realizar la carga", "Correo registrado en Cabify", "Correo registrado", "Correo Cabify"])
    saldo_amount_col = pick_first_col(df_saldo_raw, ["Monto a compensar", "Monto", "Monto a Compensar"])
    saldo_reason_col = pick_first_col(df_saldo_raw, ["Motivo compensación", "Motivo compensacion", "Motivo", "Motivo compensación ____"])

    if saldo_ticket_col is None:
        raise ValueError("No encontré la columna de ticket en la base de SALDO (ej: 'Numero ticket').")

    df_saldo = pd.DataFrame()
    df_saldo["Fecha"] = to_datetime_series(df_saldo_raw.loc[:, saldo_date_col] if saldo_date_col else pd.Series([pd.NaT]*len(df_saldo_raw)))
    df_saldo["Dirección de correo electrónico"] = safe_1d(df_saldo_raw.loc[:, saldo_email_col] if saldo_email_col else None, len(df_saldo_raw)).astype(str).str.strip()
    df_saldo["Numero"] = safe_1d(df_saldo_raw.loc[:, saldo_ticket_col], len(df_saldo_raw)).apply(extract_ticket)
    df_saldo["Correo registrado en Cabify para realizar la carga"] = safe_1d(df_saldo_raw.loc[:, saldo_cabify_mail_col] if saldo_cabify_mail_col else None, len(df_saldo_raw)).astype(str).str.strip()
    df_saldo["Monto_saldo"] = safe_1d(df_saldo_raw.loc[:, saldo_amount_col] if saldo_amount_col else None, len(df_saldo_raw)).apply(parse_clp_amount)
    df_saldo["Motivo_saldo"] = safe_1d(df_saldo_raw.loc[:, saldo_reason_col] if saldo_reason_col else None, len(df_saldo_raw)).astype(str).str.strip()

    df_saldo = df_saldo[df_saldo["Numero"].astype(str).str.len() > 0].copy()
    df_saldo["Clasificación_saldo"] = "Saldo"

    # If multiple rows per ticket (saldo), aggregate:
    df_saldo_g = (
        df_saldo.groupby("Numero", as_index=False)
        .agg({
            "Fecha": "min",
            "Dirección de correo electrónico": lambda x: next((v for v in x if str(v).strip()), ""),
            "Correo registrado en Cabify para realizar la carga": lambda x: next((v for v in x if str(v).strip()), ""),
            "Monto_saldo": "sum",
            "Motivo_saldo": lambda x: " | ".join(sorted({str(v).strip() for v in x if str(v).strip() and str(v).lower() != "nan"})),
            "Clasificación_saldo": "first",
        })
    )

    # --- TRANSFERENCIAS (filtrar Compensación Aeropuerto) ---
    # Filter column "Motivo" == "Compensación Aeropuerto"
    transfer_motivo_col = pick_first_col(df_transfer_raw, ["Motivo", "motivo"])
    transfer_date_col = pick_first_col(df_transfer_raw, ["Fecha", "Marca temporal", "marca temporal"])
    transfer_email_col = pick_first_col(df_transfer_raw, ["Dirección de correo electrónico", "Direccion de correo electronico", "Email", "Correo"])
    transfer_cabify_mail_col = pick_first_col(df_transfer_raw, ["Correo", "Correo registrado en Cabify para realizar la carga", "Correo registrado"])
    transfer_ticket_col = pick_first_col(df_transfer_raw, ["Ticket", "Numero ticket", "Número ticket", "N° ticket", "Numero"])
    transfer_amount_col = pick_first_col(df_transfer_raw, ["Monto", "Monto ", "Monto  "])
    transfer_airport_reason_col = pick_first_col(df_transfer_raw, ["Si es compensación Aeropuerto selecciona el motivo", "Si es compensacion Aeropuerto selecciona el motivo"])
    transfer_reason_fallback_col = pick_first_col(df_transfer_raw, ["Motivo de compensación", "Motivo de compensacion", "Motivo compensación", "Motivo compensacion"])
    transfer_idres_col = pick_first_col(df_transfer_raw, ["Link payments, link del viaje o numero reserva", "Link payments, link del viaje o numero reserva ", "Link payments, link del viaje o numero reserva  "])

    if transfer_ticket_col is None:
        raise ValueError("No encontré la columna 'Ticket' en la base de TRANSFERENCIAS.")

    df_transfer_f = df_transfer_raw.copy()
    if transfer_motivo_col:
        df_transfer_f = df_transfer_f[df_transfer_f[transfer_motivo_col].astype(str).str.strip() == "Compensación Aeropuerto"].copy()

    df_transfer = pd.DataFrame()
    df_transfer["Fecha"] = to_datetime_series(df_transfer_f.loc[:, transfer_date_col] if transfer_date_col else pd.Series([pd.NaT]*len(df_transfer_f)))
    df_transfer["Dirección de correo electrónico"] = safe_1d(df_transfer_f.loc[:, transfer_email_col] if transfer_email_col else None, len(df_transfer_f)).astype(str).str.strip()
    df_transfer["Numero"] = safe_1d(df_transfer_f.loc[:, transfer_ticket_col], len(df_transfer_f)).apply(extract_ticket)

    # En transferencias, "Correo" suele ser el correo del cliente (registrado en Cabify para la carga / contacto)
    df_transfer["Correo registrado en Cabify para realizar la carga"] = safe_1d(df_transfer_f.loc[:, transfer_cabify_mail_col] if transfer_cabify_mail_col else None, len(df_transfer_f)).astype(str).str.strip()

    df_transfer["Monto_transfer"] = safe_1d(df_transfer_f.loc[:, transfer_amount_col] if transfer_amount_col else None, len(df_transfer_f)).apply(parse_clp_amount)

    motivo_pref = safe_1d(df_transfer_f.loc[:, transfer_airport_reason_col] if transfer_airport_reason_col else None, len(df_transfer_f)).astype(str).str.strip()
    motivo_fb = safe_1d(df_transfer_f.loc[:, transfer_reason_fallback_col] if transfer_reason_fallback_col else None, len(df_transfer_f)).astype(str).str.strip()
    df_transfer["Motivo_transfer"] = motivo_pref.where(motivo_pref.replace({"nan": ""}).astype(str).str.strip() != "", motivo_fb)

    df_transfer["id_reserva"] = safe_1d(df_transfer_f.loc[:, transfer_idres_col] if transfer_idres_col else None, len(df_transfer_f)).apply(extract_id_reserva)

    df_transfer = df_transfer[df_transfer["Numero"].astype(str).str.len() > 0].copy()
    df_transfer["Clasificación_transfer"] = "Transferencia"

    # Aggregate transfer per ticket
    df_transfer_g = (
        df_transfer.groupby("Numero", as_index=False)
        .agg({
            "Fecha": "min",
            "Dirección de correo electrónico": lambda x: next((v for v in x if str(v).strip()), ""),
            "Correo registrado en Cabify para realizar la carga": lambda x: next((v for v in x if str(v).strip()), ""),
            "Monto_transfer": "sum",
            "Motivo_transfer": lambda x: " | ".join(sorted({str(v).strip() for v in x if str(v).strip() and str(v).lower() != "nan"})),
            "id_reserva": lambda x: next((v for v in x if str(v).strip()), ""),
            "Clasificación_transfer": "first",
        })
    )

    # --- Merge saldo + transfer by Numero (ticket) ---
    master = pd.merge(df_saldo_g, df_transfer_g, on="Numero", how="outer", suffixes=("_saldo", "_transfer"))

    # Final fields
    def coalesce(a, b):
        a = "" if a is None or (isinstance(a, float) and pd.isna(a)) else a
        b = "" if b is None or (isinstance(b, float) and pd.isna(b)) else b
        return a if str(a).strip() else b

    master["Fecha"] = master[["Fecha_saldo", "Fecha_transfer"]].min(axis=1)
    master["Dirección de correo electrónico"] = master.apply(
        lambda r: coalesce(r.get("Dirección de correo electrónico_saldo", ""), r.get("Dirección de correo electrónico_transfer", "")),
        axis=1,
    )
    master["Correo registrado en Cabify para realizar la carga"] = master.apply(
        lambda r: coalesce(r.get("Correo registrado en Cabify para realizar la carga_saldo", ""), r.get("Correo registrado en Cabify para realizar la carga_transfer", "")),
        axis=1,
    )

    master["Monto_saldo"] = master.get("Monto_saldo", 0).fillna(0.0)
    master["Monto_transfer"] = master.get("Monto_transfer", 0).fillna(0.0)
    master["Monto a compensar"] = master["Monto_saldo"] + master["Monto_transfer"]

    # Motivo unificado (union de ambos)
    def join_motivos(r):
        ms = str(r.get("Motivo_saldo", "")).strip()
        mt = str(r.get("Motivo_transfer", "")).strip()
        parts = []
        if ms and ms.lower() != "nan":
            parts.append(ms)
        if mt and mt.lower() != "nan":
            parts.append(mt)
        # unique keep order
        seen = set()
        out = []
        for p in parts:
            for x in [t.strip() for t in p.split("|")]:
                if x and x not in seen:
                    seen.add(x)
                    out.append(x)
        return " | ".join(out)

    master["Motivo compensación"] = master.apply(join_motivos, axis=1)

    # Clasificación: Saldo / Transferencia / Saldo+Transferencia
    has_saldo = master["Monto_saldo"].fillna(0) > 0
    has_trans = master["Monto_transfer"].fillna(0) > 0
    master["Clasificación"] = "Sin monto"
    master.loc[has_saldo & ~has_trans, "Clasificación"] = "Saldo"
    master.loc[~has_saldo & has_trans, "Clasificación"] = "Transferencia"
    master.loc[has_saldo & has_trans, "Clasificación"] = "Saldo+Transferencia"

    master["id_reserva"] = master.get("id_reserva", "").fillna("")

    # Keep requested columns
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

    # Make monto more readable (CLP as integer)
    out["Monto a compensar"] = out["Monto a compensar"].fillna(0.0).round(0).astype("int64")

    # Sort
    out = out.sort_values(["Fecha", "Numero"], ascending=[False, False], na_position="last")
    return out


def df_to_xlsx_bytes(df: pd.DataFrame, sheet_name: str = "master") -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return bio.getvalue()


# =========================
# UI
# =========================
st.title("Máster Compensaciones Aeropuerto (Saldo + Transferencia)")

with st.expander("Fuentes (Google Sheets)", expanded=False):
    st.write("**Carga de Saldo (Saldo):**", SALDO_SHEET_URL)
    st.write("**Compensaciones Transferencia (Transferencia):**", TRANSFER_SHEET_URL)
    st.caption("Nota: Los botones de descarga funcionan solo si el Sheet es accesible públicamente desde el servidor. Si falla, descarga manual y carga el archivo local.")

colA, colB = st.columns(2)

# Download section (Saldo)
with colA:
    st.subheader("1) Base SALDO")
    saldo_export_url = export_xlsx_url(SALDO_SHEET_URL)
    st.write("Export XLSX:", saldo_export_url)

    if "saldo_bytes" not in st.session_state:
        st.session_state["saldo_bytes"] = None
        st.session_state["saldo_checksum"] = None
        st.session_state["saldo_dl_error"] = None

    if st.button("Descargar SALDO desde Sheets", use_container_width=True):
        try:
            b = fetch_bytes(saldo_export_url)
            st.session_state["saldo_bytes"] = b
            st.session_state["saldo_checksum"] = sha256_bytes(b)
            st.session_state["saldo_dl_error"] = None
        except requests.exceptions.HTTPError as e:
            st.session_state["saldo_dl_error"] = f"HTTPError: {e}"
            st.session_state["saldo_bytes"] = None
            st.session_state["saldo_checksum"] = None
        except Exception as e:
            st.session_state["saldo_dl_error"] = f"Error: {e}"
            st.session_state["saldo_bytes"] = None
            st.session_state["saldo_checksum"] = None

    if st.session_state["saldo_dl_error"]:
        st.error("No pude descargar SALDO desde Sheets (probable falta de permisos / sheet no público).")
        st.code(st.session_state["saldo_dl_error"])
        st.link_button("Abrir SALDO en el navegador", SALDO_SHEET_URL, use_container_width=True)

    if st.session_state["saldo_bytes"]:
        st.download_button(
            "⬇️ Descargar archivo SALDO (XLSX)",
            data=st.session_state["saldo_bytes"],
            file_name="saldo.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
        st.caption(f"Checksum (sha256): {st.session_state['saldo_checksum']}")

    saldo_file = st.file_uploader("Cargar SALDO local (XLSX)", type=["xlsx"], key="saldo_upl")

# Download section (Transfer)
with colB:
    st.subheader("2) Base TRANSFERENCIA")
    transfer_export_url = export_xlsx_url(TRANSFER_SHEET_URL)
    st.write("Export XLSX:", transfer_export_url)

    if "transfer_bytes" not in st.session_state:
        st.session_state["transfer_bytes"] = None
        st.session_state["transfer_checksum"] = None
        st.session_state["transfer_dl_error"] = None

    if st.button("Descargar TRANSFERENCIA desde Sheets", use_container_width=True):
        try:
            b = fetch_bytes(transfer_export_url)
            st.session_state["transfer_bytes"] = b
            st.session_state["transfer_checksum"] = sha256_bytes(b)
            st.session_state["transfer_dl_error"] = None
        except requests.exceptions.HTTPError as e:
            st.session_state["transfer_dl_error"] = f"HTTPError: {e}"
            st.session_state["transfer_bytes"] = None
            st.session_state["transfer_checksum"] = None
        except Exception as e:
            st.session_state["transfer_dl_error"] = f"Error: {e}"
            st.session_state["transfer_bytes"] = None
            st.session_state["transfer_checksum"] = None

    if st.session_state["transfer_dl_error"]:
        st.error("No pude descargar TRANSFERENCIA desde Sheets (probable falta de permisos / sheet no público).")
        st.code(st.session_state["transfer_dl_error"])
        st.link_button("Abrir TRANSFERENCIA en el navegador", TRANSFER_SHEET_URL, use_container_width=True)

    if st.session_state["transfer_bytes"]:
        st.download_button(
            "⬇️ Descargar archivo TRANSFERENCIA (XLSX)",
            data=st.session_state["transfer_bytes"],
            file_name="transferencia.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
        st.caption(f"Checksum (sha256): {st.session_state['transfer_checksum']}")

    transfer_file = st.file_uploader("Cargar TRANSFERENCIA local (XLSX)", type=["xlsx"], key="transfer_upl")

st.divider()
st.subheader("3) Construcción del Máster")

# Decide input sources
def get_df_from_choice(uploaded_file, downloaded_bytes, label):
    """
    Prefer uploaded local file if provided, else fallback to downloaded bytes.
    """
    if uploaded_file is not None:
        b = uploaded_file.getvalue()
        return read_excel_any(b), b
    if downloaded_bytes is not None:
        return read_excel_any(downloaded_bytes), downloaded_bytes
    return None, None


df_saldo_raw, saldo_used_bytes = get_df_from_choice(saldo_file, st.session_state["saldo_bytes"], "SALDO")
df_transfer_raw, transfer_used_bytes = get_df_from_choice(transfer_file, st.session_state["transfer_bytes"], "TRANSFER")

# Checksum compare (simple)
def checksum_notice(uploaded_file, downloaded_checksum, kind):
    if uploaded_file is None or not downloaded_checksum:
        return
    upl_hash = sha256_bytes(uploaded_file.getvalue())
    if upl_hash != downloaded_checksum:
        st.warning(
            f"⚠️ Checksum no coincide para **{kind}**. "
            f"Local={upl_hash[:12]}… vs Sheets={downloaded_checksum[:12]}… "
            f"(Esto sugiere que el archivo local no es la última versión del Sheet)."
        )
    else:
        st.success(f"✅ Checksum coincide para **{kind}** (local = Sheets).")

checksum_notice(saldo_file, st.session_state.get("saldo_checksum"), "SALDO")
checksum_notice(transfer_file, st.session_state.get("transfer_checksum"), "TRANSFERENCIA")

if df_saldo_raw is None or df_transfer_raw is None:
    st.info("Carga ambas bases (o descarga desde Sheets si se puede) para construir el máster.")
else:
    try:
        master = build_master(df_saldo_raw, df_transfer_raw)

        # Quick KPIs
        c1, c2, c3 = st.columns(3)
        c1.metric("Tickets únicos", int(master["Numero"].nunique()))
        c2.metric("Registros (máster)", int(len(master)))
        c3.metric("Tickets Saldo+Transfer", int((master["Clasificación"] == "Saldo+Transferencia").sum()))

        st.dataframe(master, use_container_width=True, height=520)

        xlsx_master = df_to_xlsx_bytes(master, "master")
        st.download_button(
            "⬇️ Descargar Máster (XLSX)",
            data=xlsx_master,
            file_name="master_compensaciones_aeropuerto.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    except Exception as e:
        st.error("Se produjo un error construyendo el máster. Tip: revisa que las columnas esperadas existan en tus archivos.")
        st.code(f"{type(e).__name__}: {e}")
        st.caption("Si quieres, pega aquí (texto) los encabezados exactos de cada archivo y lo adapto a tu caso real.")
