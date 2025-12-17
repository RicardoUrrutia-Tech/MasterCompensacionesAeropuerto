import io
import re
import hashlib
from urllib.parse import urlparse, parse_qs

import numpy as np
import pandas as pd
import requests
import streamlit as st


# =========================
# Config
# =========================
st.set_page_config(page_title="Máster Compensaciones Aeropuerto", layout="wide")

SALDO_SHEET_URL = "https://docs.google.com/spreadsheets/d/1Yj8q2dnlKqIZ1-vdr7wZZp_jvXiLoYO6Qwc8NeIeUnE/edit?gid=1139202449#gid=1139202449"
TRANSFER_SHEET_URL = "https://docs.google.com/spreadsheets/d/1yHTfTOD-N8VYBSzQRCkaNpMpAQHykBzVB5mYsXS6rHs/edit?resourcekey=&gid=1627777729#gid=1627777729"

USER_AGENT = "Mozilla/5.0 (compatible; MasterCompensaciones/1.0; +https://streamlit.io)"


# =========================
# Helpers: URLs / bytes / checksum
# =========================
def md5_bytes(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def parse_sheet_id_and_gid(sheet_url: str):
    """
    Extract spreadsheet ID and gid from a Google Sheets URL.
    """
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
    sheet_id = m.group(1) if m else None

    # gid can appear as query param or fragment
    gid = None
    try:
        parsed = urlparse(sheet_url)
        qs = parse_qs(parsed.query)
        if "gid" in qs:
            gid = qs["gid"][0]
        if gid is None and parsed.fragment:
            # fragment might contain gid=...
            frag_qs = parse_qs(parsed.fragment)
            if "gid" in frag_qs:
                gid = frag_qs["gid"][0]
            else:
                m2 = re.search(r"gid=(\d+)", parsed.fragment)
                gid = m2.group(1) if m2 else None
    except Exception:
        gid = None

    return sheet_id, gid


def build_export_url(sheet_url: str, fmt: str = "xlsx") -> str:
    """
    Build a direct export URL.
    fmt: xlsx or csv (csv requires gid ideally; if missing, exports first sheet)
    """
    sheet_id, gid = parse_sheet_id_and_gid(sheet_url)
    if not sheet_id:
        raise ValueError("No se pudo extraer el spreadsheetId desde la URL.")

    if fmt == "xlsx":
        # xlsx ignores gid sometimes, but we include it (common pattern).
        return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx&gid={gid or 0}"
    elif fmt == "csv":
        # csv typically needs gid to pick the tab
        if gid:
            return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
        return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
    else:
        raise ValueError("Formato no soportado.")


def fetch_bytes(url: str, timeout: int = 30) -> bytes:
    """
    Download file bytes. Raises HTTPError on non-2xx.
    """
    headers = {"User-Agent": USER_AGENT}
    r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    return r.content


# =========================
# Helpers: column picking / safe series
# =========================
def norm_colname(s: str) -> str:
    s = str(s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def pick_col(df: pd.DataFrame, candidates):
    """
    Pick the first matching column by normalized name (exact or contains).
    candidates: list of strings
    """
    if df is None or df.empty:
        return None
    cols = list(df.columns)
    nmap = {c: norm_colname(c) for c in cols}

    cand_norm = [norm_colname(x) for x in candidates]

    # exact match
    for c in cols:
        if nmap[c] in cand_norm:
            return c

    # contains match (useful if excel renames with .1 etc.)
    for cn in cand_norm:
        for c in cols:
            if cn and cn in nmap[c]:
                return c
    return None


def ensure_series(x, length=None) -> pd.Series:
    """
    Ensure x is a 1D Series (if DataFrame due to duplicated column names, take first col).
    """
    if isinstance(x, pd.DataFrame):
        x = x.iloc[:, 0]
    if isinstance(x, pd.Index):
        x = pd.Series(list(x))
    if not isinstance(x, pd.Series):
        x = pd.Series(x)

    if length is not None and len(x) != length:
        # align by reindexing (best-effort)
        x = x.reset_index(drop=True)
        if len(x) < length:
            x = x.reindex(range(length))
        else:
            x = x.iloc[:length]
    return x


def to_datetime_series(x: pd.Series) -> pd.Series:
    """
    Robust datetime parse for typical Sheets/Excel exports.
    """
    x = ensure_series(x)
    # Try direct parse
    dt = pd.to_datetime(x, errors="coerce", dayfirst=True)

    # Sometimes numeric excel serial dates appear
    if dt.isna().all():
        numeric = pd.to_numeric(x, errors="coerce")
        dt2 = pd.to_datetime(numeric, unit="D", origin="1899-12-30", errors="coerce")
        dt = dt2
    return dt


# =========================
# Parsing business fields
# =========================
def parse_ticket_value(v) -> str:
    """
    Ticket could be:
    - "66171869"
    - "#66185638"
    - "https://cabify.zendesk.com/agent/tickets/67587605" (take last / part)
    """
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return ""
    s = str(v).strip()

    if not s or s.lower() in {"nan", "none"}:
        return ""

    # If URL, take last segment
    if "http://" in s.lower() or "https://" in s.lower():
        # remove trailing slashes
        s2 = s.rstrip("/")
        last = s2.split("/")[-1]
        # sometimes includes querystring
        last = last.split("?")[0].strip()
        # remove non-digits but keep if digits exist
        digits = re.findall(r"\d+", last)
        return digits[0] if digits else last

    # Remove leading '#'
    s = s.replace("#", " ").strip()

    # Extract first digits sequence (ticket is numeric)
    digits = re.findall(r"\d+", s)
    return digits[0] if digits else s


def parse_amount(v) -> float:
    """
    Normalize amounts that may look like:
      19980
      497.572   (CL thousands separator)
      14.968
      $28.47    (user reports it actually means 28.471 -> 28471)
    Heuristics:
    - Remove currency symbols/spaces.
    - If has both '.' and ',' -> assume '.' thousands, ',' decimals.
    - If only '.' and last group len==3 -> thousands separators, remove dots.
    - If only '.' and last group len==2 and value < 1000 -> treat as "thousands-like" and multiply by 1000.
    """
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return 0.0

    if isinstance(v, (int, float, np.integer, np.floating)) and not isinstance(v, bool):
        # already numeric
        try:
            return float(v)
        except Exception:
            return 0.0

    s = str(v).strip()
    if not s or s.lower() in {"nan", "none"}:
        return 0.0

    s = s.replace("$", "").replace("CLP", "").replace("clp", "")
    s = s.replace(" ", "")

    # If it's something like "28.471" (as text), this logic will handle.
    if "," in s and "." in s:
        # assume 1.234,56
        s2 = s.replace(".", "").replace(",", ".")
        try:
            return float(s2)
        except Exception:
            return 0.0

    if "," in s and "." not in s:
        # could be decimals with comma, or thousands with comma; assume decimals
        s2 = s.replace(".", "").replace(",", ".")
        try:
            return float(s2)
        except Exception:
            return 0.0

    if "." in s and "," not in s:
        parts = s.split(".")
        # remove empty parts
        parts = [p for p in parts if p != ""]
        if len(parts) == 1:
            try:
                return float(parts[0])
            except Exception:
                return 0.0

        last = parts[-1]
        # thousands separator pattern: groups of 3
        if len(last) == 3:
            s2 = "".join(parts)
            try:
                return float(s2)
            except Exception:
                return 0.0

        # ambiguous case: e.g. "28.47" in CLP context
        # If looks like 2 decimals and base value < 1000, multiply by 1000
        if len(last) == 2:
            try:
                val = float(s)
                if val < 1000:
                    return float(int(round(val * 1000)))
                return val
            except Exception:
                # fallback: remove dots
                s2 = "".join(parts)
                try:
                    return float(s2)
                except Exception:
                    return 0.0

        # fallback: remove dots
        s2 = "".join(parts)
        try:
            return float(s2)
        except Exception:
            return 0.0

    # plain number
    try:
        return float(s)
    except Exception:
        # try extract digits
        digs = re.findall(r"\d+", s)
        return float(digs[0]) if digs else 0.0


def extract_id_reserva(v) -> str:
    """
    From 'Link payments, link del viaje o numero reserva'.
    Could be a URL (take last meaningful segment) or a raw reservation number.
    """
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return ""
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none"}:
        return ""

    if "http://" in s.lower() or "https://" in s.lower():
        s2 = s.rstrip("/")
        last = s2.split("/")[-1]
        last = last.split("?")[0].strip()
        return last

    return s


# =========================
# Reading files (downloaded or uploaded)
# =========================
def read_table_from_bytes(file_bytes: bytes, filename_hint: str = "file.xlsx") -> pd.DataFrame:
    """
    Read xlsx or csv bytes into DataFrame.
    """
    name = filename_hint.lower()
    bio = io.BytesIO(file_bytes)

    if name.endswith(".csv"):
        return pd.read_csv(bio)
    # default xlsx
    return pd.read_excel(bio)


def read_uploaded(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    b = uploaded_file.getvalue()
    return read_table_from_bytes(b, filename_hint=name)


# =========================
# Build master
# =========================
def build_master(df_saldo_raw: pd.DataFrame, df_transfer_raw: pd.DataFrame) -> pd.DataFrame:
    # --------- SALDO base ----------
    # Expected-ish columns:
    # Marca temporal / Fecha, Dirección de correo electrónico, Numero ticket,
    # Correo registrado..., Monto a compensar, Motivo compensación
    date_col = pick_col(df_saldo_raw, ["marca temporal", "fecha", "timestamp"])
    email_col = pick_col(df_saldo_raw, ["dirección de correo electrónico", "direccion de correo electronico", "email", "correo"])
    ticket_col = pick_col(df_saldo_raw, ["numero ticket", "número ticket", "ticket", "numero de ticket"])
    correo_reg_col = pick_col(df_saldo_raw, ["correo registrado en cabify para realizar la carga", "correo registrado", "correo registrado en cabify"])
    monto_col = pick_col(df_saldo_raw, ["monto a compensar", "monto", "importe"])
    motivo_col = pick_col(df_saldo_raw, ["motivo compensación", "motivo compensacion", "motivo"])

    n_s = len(df_saldo_raw)
    df_saldo = pd.DataFrame({
        "Fecha": to_datetime_series(df_saldo_raw[date_col]) if date_col else pd.Series([pd.NaT] * n_s),
        "Dirección de correo electrónico": ensure_series(df_saldo_raw[email_col], n_s) if email_col else pd.Series([""] * n_s),
        "Numero": ensure_series(df_saldo_raw[ticket_col], n_s).map(parse_ticket_value) if ticket_col else pd.Series([""] * n_s),
        "Correo registrado en Cabify para realizar la carga": ensure_series(df_saldo_raw[correo_reg_col], n_s) if correo_reg_col else pd.Series([""] * n_s),
        "Monto_saldo": ensure_series(df_saldo_raw[monto_col], n_s).map(parse_amount) if monto_col else pd.Series([0.0] * n_s),
        "Motivo_saldo": ensure_series(df_saldo_raw[motivo_col], n_s) if motivo_col else pd.Series([""] * n_s),
    })

    df_saldo["Clasificación_base"] = "Saldo"
    df_saldo["id_reserva"] = ""

    # --------- TRANSFER base ----------
    # We only want rows where Motivo == "Compensación Aeropuerto"
    motivo2_col = pick_col(df_transfer_raw, ["motivo", "motivo de compensación", "motivo compensacion"])
    airport_motivo_col = pick_col(df_transfer_raw, ["si es compensación aeropuerto selecciona el motivo", "si es compensacion aeropuerto selecciona el motivo"])
    date2_col = pick_col(df_transfer_raw, ["fecha", "marca temporal", "timestamp"])
    email2_col = pick_col(df_transfer_raw, ["dirección de correo electrónico", "direccion de correo electronico", "email", "correo"])
    correo2_col = pick_col(df_transfer_raw, ["correo"])
    ticket2_col = pick_col(df_transfer_raw, ["ticket", "numero ticket", "número ticket"])
    monto2_col = pick_col(df_transfer_raw, ["monto"])  # there are two "Monto" columns sometimes; pick_col handles contains
    id_reserva_col = pick_col(df_transfer_raw, ["link payments, link del viaje o numero reserva", "link payments", "link del viaje o numero reserva"])

    df_transfer_f = df_transfer_raw.copy()

    if motivo2_col:
        mask = df_transfer_f[motivo2_col].astype(str).str.strip().str.lower().eq("compensación aeropuerto".lower()) | \
               df_transfer_f[motivo2_col].astype(str).str.strip().str.lower().eq("compensacion aeropuerto".lower())
        df_transfer_f = df_transfer_f.loc[mask].copy()

    n_t = len(df_transfer_f)

    df_transfer = pd.DataFrame({
        "Fecha": to_datetime_series(df_transfer_f[date2_col]) if date2_col else pd.Series([pd.NaT] * n_t),
        "Dirección de correo electrónico": ensure_series(df_transfer_f[email2_col], n_t) if email2_col else pd.Series([""] * n_t),
        "Numero": ensure_series(df_transfer_f[ticket2_col], n_t).map(parse_ticket_value) if ticket2_col else pd.Series([""] * n_t),
        "Correo registrado en Cabify para realizar la carga": ensure_series(df_transfer_f[correo2_col], n_t) if correo2_col else pd.Series([""] * n_t),
        "Monto_transfer": ensure_series(df_transfer_f[monto2_col], n_t).map(parse_amount) if monto2_col else pd.Series([0.0] * n_t),
        "Motivo_transfer": ensure_series(df_transfer_f[airport_motivo_col], n_t) if airport_motivo_col else pd.Series([""] * n_t),
        "id_reserva": ensure_series(df_transfer_f[id_reserva_col], n_t).map(extract_id_reserva) if id_reserva_col else pd.Series([""] * n_t),
    })

    df_transfer["Clasificación_base"] = "Transferencia"

    # --------- Clean / standardize keys ----------
    df_saldo["Numero"] = df_saldo["Numero"].astype(str).str.strip()
    df_transfer["Numero"] = df_transfer["Numero"].astype(str).str.strip()

    # Remove empty ticket rows
    df_saldo = df_saldo.loc[df_saldo["Numero"].ne("")].copy()
    df_transfer = df_transfer.loc[df_transfer["Numero"].ne("")].copy()

    # --------- Merge logic: one row per ticket ----------
    # We aggregate each base per ticket first (in case duplicates inside same base).
    agg_s = (df_saldo
             .groupby("Numero", dropna=False, as_index=False)
             .agg({
                 "Fecha": "min",
                 "Dirección de correo electrónico": "first",
                 "Correo registrado en Cabify para realizar la carga": "first",
                 "Monto_saldo": "sum",
                 "Motivo_saldo": lambda x: next((str(v).strip() for v in x if str(v).strip() and str(v).lower() not in {"nan", "none"}), "")
             }))

    agg_t = (df_transfer
             .groupby("Numero", dropna=False, as_index=False)
             .agg({
                 "Fecha": "min",
                 "Dirección de correo electrónico": "first",
                 "Correo registrado en Cabify para realizar la carga": "first",
                 "Monto_transfer": "sum",
                 "Motivo_transfer": lambda x: next((str(v).strip() for v in x if str(v).strip() and str(v).lower() not in {"nan", "none"}), ""),
                 "id_reserva": lambda x: next((str(v).strip() for v in x if str(v).strip() and str(v).lower() not in {"nan", "none"}), "")
             }))

    merged = pd.merge(agg_s, agg_t, on="Numero", how="outer", suffixes=("_saldo", "_transfer"))

    # Fecha: prefer earliest non-null
    merged["Fecha"] = merged[["Fecha_saldo", "Fecha_transfer"]].min(axis=1)

    # Email: prefer saldo if exists else transfer
    merged["Dirección de correo electrónico"] = merged["Dirección de correo electrónico_saldo"].fillna("").astype(str).str.strip()
    merged.loc[merged["Dirección de correo electrónico"].eq(""), "Dirección de correo electrónico"] = (
        merged["Dirección de correo electrónico_transfer"].fillna("").astype(str).str.strip()
    )

    # Correo registrado: prefer saldo else transfer
    merged["Correo registrado en Cabify para realizar la carga"] = merged["Correo registrado en Cabify para realizar la carga_saldo"].fillna("").astype(str).str.strip()
    merged.loc[merged["Correo registrado en Cabify para realizar la carga"].eq(""), "Correo registrado en Cabify para realizar la carga"] = (
        merged["Correo registrado en Cabify para realizar la carga_transfer"].fillna("").astype(str).str.strip()
    )

    # Monto total
    merged["Monto a compensar"] = merged["Monto_saldo"].fillna(0.0) + merged["Monto_transfer"].fillna(0.0)

    # Motivo compensación unificado:
    # prefer saldo motive; else airport motive from transfer
    merged["Motivo compensación"] = merged["Motivo_saldo"].fillna("").astype(str).str.strip()
    merged.loc[merged["Motivo compensación"].eq(""), "Motivo compensación"] = (
        merged["Motivo_transfer"].fillna("").astype(str).str.strip()
    )

    # id_reserva from transfer (currently)
    merged["id_reserva"] = merged["id_reserva"].fillna("").astype(str).str.strip()

    # Clasificación
    has_s = merged["Monto_saldo"].fillna(0).gt(0)
    has_t = merged["Monto_transfer"].fillna(0).gt(0)
    merged["Clasificación"] = np.select(
        [has_s & has_t, has_s & ~has_t, ~has_s & has_t],
        ["Mixta", "Saldo", "Transferencia"],
        default="Transferencia"
    )

    # Final columns
    out = merged[[
        "Fecha",
        "Dirección de correo electrónico",
        "Numero",
        "Correo registrado en Cabify para realizar la carga",
        "Monto a compensar",
        "Motivo compensación",
        "id_reserva",
        "Clasificación",
    ]].copy()

    # Order / types
    out["Numero"] = out["Numero"].astype(str).str.strip()

    # Optional: sort by date desc then ticket
    out = out.sort_values(by=["Fecha", "Numero"], ascending=[False, True], na_position="last").reset_index(drop=True)

    return out


# =========================
# UI
# =========================
st.title("Máster Compensaciones Aeropuerto")
st.caption("Une compensaciones por Saldo + Transferencias (filtrando 'Compensación Aeropuerto') y deja 1 registro por ticket.")

with st.expander("Fuentes (Google Sheets)", expanded=True):
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Carga de Saldo (Sheet)**")
        st.write(SALDO_SHEET_URL)
    with c2:
        st.markdown("**Compensaciones Transferencia (Sheet)**")
        st.write(TRANSFER_SHEET_URL)

st.divider()

# Download section
st.subheader("1) Descargar desde Google Sheets (botones)")
download_col1, download_col2 = st.columns(2)

if "saldo_bytes" not in st.session_state:
    st.session_state["saldo_bytes"] = None
if "transfer_bytes" not in st.session_state:
    st.session_state["transfer_bytes"] = None

with download_col1:
    st.markdown("**Saldo**")
    saldo_export_url = build_export_url(SALDO_SHEET_URL, "xlsx")
    if st.button("Descargar Saldo (XLSX)", use_container_width=True):
        try:
            st.session_state["saldo_bytes"] = fetch_bytes(saldo_export_url)
            st.success("Saldo descargado OK.")
        except requests.exceptions.HTTPError as e:
            st.session_state["saldo_bytes"] = None
            st.error(
                "No pude descargar el Sheet de Saldo (HTTPError). "
                "Esto suele pasar si el Sheet no es público o requiere login.\n\n"
                "Solución típica: publicar/compartir el Sheet con acceso 'Cualquiera con el enlace (lector)' "
                "o usar carga local."
            )
            st.exception(e)
        except Exception as e:
            st.session_state["saldo_bytes"] = None
            st.error("Error inesperado descargando Saldo.")
            st.exception(e)

    if st.session_state["saldo_bytes"]:
        st.download_button(
            "Bajar archivo Saldo descargado",
            data=st.session_state["saldo_bytes"],
            file_name="saldo.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
        st.caption(f"MD5 (Saldo descargado): `{md5_bytes(st.session_state['saldo_bytes'])}`")

with download_col2:
    st.markdown("**Transferencias (filtraremos Compensación Aeropuerto al construir el máster)**")
    transfer_export_url = build_export_url(TRANSFER_SHEET_URL, "xlsx")
    if st.button("Descargar Transferencias (XLSX)", use_container_width=True):
        try:
            st.session_state["transfer_bytes"] = fetch_bytes(transfer_export_url)
            st.success("Transferencias descargadas OK.")
        except requests.exceptions.HTTPError as e:
            st.session_state["transfer_bytes"] = None
            st.error(
                "No pude descargar el Sheet de Transferencias (HTTPError). "
                "Esto suele pasar si el Sheet no es público o requiere login.\n\n"
                "Solución típica: publicar/compartir el Sheet con acceso 'Cualquiera con el enlace (lector)' "
                "o usar carga local."
            )
            st.exception(e)
        except Exception as e:
            st.session_state["transfer_bytes"] = None
            st.error("Error inesperado descargando Transferencias.")
            st.exception(e)

    if st.session_state["transfer_bytes"]:
        st.download_button(
            "Bajar archivo Transferencias descargado",
            data=st.session_state["transfer_bytes"],
            file_name="transferencias.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
        st.caption(f"MD5 (Transferencias descargado): `{md5_bytes(st.session_state['transfer_bytes'])}`")

st.divider()

st.subheader("2) Cargar archivos localmente")
u1, u2 = st.columns(2)
with u1:
    up_saldo = st.file_uploader("Sube archivo Saldo (XLSX/CSV)", type=["xlsx", "csv"], key="up_saldo")
with u2:
    up_transfer = st.file_uploader("Sube archivo Transferencias (XLSX/CSV)", type=["xlsx", "csv"], key="up_transfer")

# Checksum compare
st.subheader("3) Checksum simple (local vs sheet descargado)")
chk1, chk2 = st.columns(2)

with chk1:
    if up_saldo is not None:
        local_saldo_md5 = md5_bytes(up_saldo.getvalue())
        st.write(f"MD5 (Saldo local): `{local_saldo_md5}`")
        if st.session_state["saldo_bytes"]:
            remote_saldo_md5 = md5_bytes(st.session_state["saldo_bytes"])
            if local_saldo_md5 == remote_saldo_md5:
                st.success("Saldo local coincide con el descargado del Sheet.")
            else:
                st.warning("Saldo local NO coincide con el descargado del Sheet.")
        else:
            st.info("No hay Saldo descargado del Sheet para comparar (usa botón de descarga o deja así).")
    else:
        st.caption("Sube Saldo local para ver MD5.")

with chk2:
    if up_transfer is not None:
        local_transfer_md5 = md5_bytes(up_transfer.getvalue())
        st.write(f"MD5 (Transfer local): `{local_transfer_md5}`")
        if st.session_state["transfer_bytes"]:
            remote_transfer_md5 = md5_bytes(st.session_state["transfer_bytes"])
            if local_transfer_md5 == remote_transfer_md5:
                st.success("Transfer local coincide con el descargado del Sheet.")
            else:
                st.warning("Transfer local NO coincide con el descargado del Sheet.")
        else:
            st.info("No hay Transfer descargado del Sheet para comparar (usa botón de descarga o deja así).")
    else:
        st.caption("Sube Transfer local para ver MD5.")

st.divider()

st.subheader("4) Construir Máster (1 registro por ticket)")

def resolve_df(suggested_bytes, uploaded_file, default_name):
    """
    Priority:
    1) If user uploaded a file -> use that
    2) else if downloaded bytes exist -> use that
    """
    if uploaded_file is not None:
        df = read_uploaded(uploaded_file)
        src = f"local ({uploaded_file.name})"
        return df, src

    if suggested_bytes:
        df = read_table_from_bytes(suggested_bytes, filename_hint=default_name)
        src = f"sheet descargado ({default_name})"
        return df, src

    return None, None


df_saldo_raw, src_saldo = resolve_df(st.session_state["saldo_bytes"], up_saldo, "saldo.xlsx")
df_transfer_raw, src_transfer = resolve_df(st.session_state["transfer_bytes"], up_transfer, "transferencias.xlsx")

cols_a, cols_b = st.columns(2)
with cols_a:
    st.markdown("**Fuente Saldo usada:** " + (src_saldo or "_(no cargada)_"))
with cols_b:
    st.markdown("**Fuente Transfer usada:** " + (src_transfer or "_(no cargada)_"))

build_btn = st.button("Construir Máster", type="primary", use_container_width=True)

if build_btn:
    if df_saldo_raw is None or df_transfer_raw is None:
        st.error("Falta cargar/descargar uno o ambos archivos (Saldo y Transferencias).")
    else:
        try:
            master = build_master(df_saldo_raw, df_transfer_raw)
            st.success(f"Máster construido: {len(master):,} registros (tickets únicos).")

            st.markdown("### Vista previa")
            st.dataframe(master, use_container_width=True, height=420)

            # Downloads
            out_xlsx = io.BytesIO()
            with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
                master.to_excel(writer, index=False, sheet_name="master")
            out_xlsx.seek(0)

            out_csv = master.to_csv(index=False).encode("utf-8")

            d1, d2 = st.columns(2)
            with d1:
                st.download_button(
                    "Descargar Máster (XLSX)",
                    data=out_xlsx.getvalue(),
                    file_name="master_compensaciones.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            with d2:
                st.download_button(
                    "Descargar Máster (CSV)",
                    data=out_csv,
                    file_name="master_compensaciones.csv",
                    mime="text/csv",
                    use_container_width=True
                )

            st.info(
                "Nota montos: si algún monto viene como texto tipo `28.47` en un contexto CLP, "
                "se aplica una heurística para interpretarlo como miles (multiplica por 1000). "
                "Si el export ya viene numérico (lo más común), se respeta el valor."
            )

        except Exception as e:
            st.error("Se produjo un error construyendo el máster. Tip: revisa que las columnas esperadas existan en tus archivos.")
            st.write("Detalle:", repr(e))
            st.exception(e)

st.divider()

with st.expander("Diagnóstico rápido (ver nombres de columnas detectadas)", expanded=False):
    if df_saldo_raw is not None:
        st.markdown("**Saldo - columnas**")
        st.write(list(df_saldo_raw.columns))
    else:
        st.caption("Saldo no cargado.")
    if df_transfer_raw is not None:
        st.markdown("**Transferencias - columnas**")
        st.write(list(df_transfer_raw.columns))
    else:
        st.caption("Transferencias no cargadas.")

