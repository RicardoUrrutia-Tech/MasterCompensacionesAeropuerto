import io
import re
import hashlib
from datetime import datetime

import numpy as np
import pandas as pd
import requests
import streamlit as st


# =========================
# Config
# =========================
st.set_page_config(page_title="Máster de Compensaciones", layout="wide")

SHEET_SALDO_URL = "https://docs.google.com/spreadsheets/d/1Yj8q2dnlKqIZ1-vdr7wZZp_jvXiLoYO6Qwc8NeIeUnE/edit?gid=1139202449#gid=1139202449"
SHEET_TRANSF_URL = "https://docs.google.com/spreadsheets/d/1yHTfTOD-N8VYBSzQRCkaNpMpAQHykBzVB5mYsXS6rHs/edit?resourcekey=&gid=1627777729#gid=1627777729"


# =========================
# Helpers (Sheets download + checksum)
# =========================
def parse_sheet_id_and_gid(sheet_url: str):
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
    if not m:
        return None, None
    sheet_id = m.group(1)
    gid = None
    mgid = re.search(r"gid=([0-9]+)", sheet_url)
    if mgid:
        gid = mgid.group(1)
    return sheet_id, gid


def build_export_xlsx_url(sheet_url: str) -> str:
    sheet_id, gid = parse_sheet_id_and_gid(sheet_url)
    if not sheet_id:
        raise ValueError("No pude extraer el sheet_id desde el link.")
    # xlsx export (gid opcional, pero lo incluimos para apuntar a la pestaña)
    if gid:
        return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx&gid={gid}"
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def fetch_bytes(url: str, timeout: int = 60) -> bytes:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.content


def read_excel_from_bytes(b: bytes) -> pd.DataFrame:
    bio = io.BytesIO(b)
    return pd.read_excel(bio, engine="openpyxl")


def safe_strip(s):
    if pd.isna(s):
        return None
    if isinstance(s, str):
        t = s.strip()
        return t if t else None
    return s


# =========================
# Normalización de Ticket
# =========================
def normalize_ticket(value) -> str | None:
    """
    Acepta:
      - 66185638
      - #66185638
      - https://cabify.zendesk.com/agent/tickets/67587605 (toma lo último tras /)
    Devuelve solo dígitos (string) o None.
    """
    if pd.isna(value):
        return None
    s = str(value).strip()

    if not s:
        return None

    # Si es URL, toma el último tramo
    if "http://" in s or "https://" in s:
        s = s.rstrip("/")
        s = s.split("/")[-1]

    # Quitar #
    s = s.replace("#", "").strip()

    # Extraer dígitos
    m = re.search(r"(\d+)", s)
    return m.group(1) if m else None


# =========================
# Normalización de Monto
# =========================
def parse_amount(value) -> float | None:
    """
    Intenta convertir montos con distintos formatos:
    - "19980"
    - "14.968" => 14968 (punto como miles)
    - "497.572" => 497572
    - "$28.47" (pero en sheet a veces es 28.471) => heurística a 28471
    - números (float/int) exportados desde xlsx

    Retorna float (idealmente entero en CLP) o None.
    """
    if pd.isna(value):
        return None

    # Caso numérico ya parseado por Excel
    if isinstance(value, (int, np.integer)):
        return float(value)

    if isinstance(value, (float, np.floating)):
        v = float(value)
        if np.isnan(v):
            return None
        # Heurística: si viene con 3 decimales y es "pequeño", suele ser miles mal formateados (28.471 -> 28471)
        # Ej: 28.471, 14.968, 497.572
        frac = abs(v - round(v))
        # Detectar "tres decimales relevantes"
        if v < 1000 and frac > 0 and round(v, 3) == v:
            return float(round(v * 1000))
        # Si es grande con 3 decimales (poco probable como CLP), igual lo tratamos como miles si termina en 3 decimales exactos
        if v >= 1000 and round(v, 3) == v and frac > 0:
            return float(round(v * 1000))
        return float(round(v))

    # Caso string
    s = str(value).strip()
    if not s:
        return None

    # Quitar símbolos y espacios
    s = s.replace("$", "").replace("CLP", "").replace("clp", "").replace(" ", "")

    # Si tiene coma y punto: asumimos coma decimal y punto miles (formato común)
    # "1.234,56" -> 1234.56
    if "," in s and "." in s:
        s2 = s.replace(".", "").replace(",", ".")
        try:
            return float(s2)
        except Exception:
            pass

    # Si tiene solo coma: puede ser decimal -> "1234,5" => 1234.5
    if "," in s and "." not in s:
        try:
            return float(s.replace(",", "."))
        except Exception:
            pass

    # Si tiene solo puntos: normalmente miles -> "14.968" => 14968
    # pero también puede ser "28.47" que en realidad era "28.471" en el sheet.
    if "." in s and "," not in s:
        parts = s.split(".")
        if len(parts) == 2 and len(parts[1]) == 2:
            # Caso raro tipo 28.47 -> lo interpretamos como 28470 aprox? (pero tú indicas que era 28.471 -> 28471).
            # Heurística: multiplicar por 1000 y redondear
            try:
                v = float(s)
                return float(round(v * 1000))
            except Exception:
                pass

        # Miles: remover puntos
        s2 = s.replace(".", "")
        if s2.isdigit():
            return float(s2)

        # fallback
        try:
            v = float(s)
            # si quedó decimal con 3 decimales, miles
            if round(v, 3) == v and (abs(v - round(v)) > 0):
                return float(round(v * 1000))
            return float(round(v))
        except Exception:
            return None

    # Solo dígitos
    if s.isdigit():
        return float(s)

    # Último intento: extraer número
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if m:
        try:
            v = float(m.group(1))
            return float(round(v))
        except Exception:
            return None

    return None


# =========================
# Column mapping
# =========================
def pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = list(df.columns)
    lower = {str(c).strip().lower(): c for c in cols}
    for cand in candidates:
        k = cand.strip().lower()
        if k in lower:
            return lower[k]
    return None


def coalesce_cols(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """
    Soporta duplicados tipo 'Monto' y 'Monto.1'
    """
    cols = list(df.columns)
    lc = [str(c).strip().lower() for c in cols]
    for cand in candidates:
        base = cand.strip().lower()
        # match exact
        for i, c in enumerate(lc):
            if c == base:
                return cols[i]
        # match startswith for duplicates (monto.1)
        for i, c in enumerate(lc):
            if c.startswith(base + "."):
                return cols[i]
    return None


# =========================
# Transform: Saldo
# =========================
def transform_saldo(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()

    # Intentar detectar columnas esperadas
    col_fecha = coalesce_cols(df, ["Marca temporal", "Fecha", "Timestamp"])
    col_agent = coalesce_cols(df, ["Dirección de correo electrónico", "Direccion de correo electronico", "Email"])
    col_ticket = coalesce_cols(df, ["Numero ticket", "Número ticket", "Ticket", "Numero", "N° ticket"])
    col_correo_carga = coalesce_cols(df, ["Correo registrado en Cabify para realizar la carga", "Correo registrado", "Correo"])
    col_monto = coalesce_cols(df, ["Monto a compensar", "Monto", "Monto a compensar "])
    col_motivo = coalesce_cols(df, ["Motivo compensación", "Motivo compensacion", "Motivo"])

    missing = [("Fecha", col_fecha), ("Email agente", col_agent), ("Ticket", col_ticket), ("Correo carga", col_correo_carga), ("Monto", col_monto), ("Motivo", col_motivo)]
    miss_names = [n for n, c in missing if c is None]
    if miss_names:
        raise ValueError(f"[Saldo] No pude identificar estas columnas: {', '.join(miss_names)}. Revisa encabezados del archivo.")

    out = pd.DataFrame()
    out["Fecha"] = pd.to_datetime(df[col_fecha], errors="coerce", dayfirst=True)
    out["Dirección de correo electrónico"] = df[col_agent].map(safe_strip)
    out["Numero"] = df[col_ticket].map(normalize_ticket)
    out["Correo registrado en Cabify para realizar la carga"] = df[col_correo_carga].map(safe_strip)
    out["Monto_saldo"] = df[col_monto].map(parse_amount)
    out["Motivo_saldo"] = df[col_motivo].map(safe_strip)

    out["Fuente_saldo"] = True
    return out


# =========================
# Transform: Transferencia (filtrada a Aeropuerto)
# =========================
def transform_transfer(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()

    col_fecha = coalesce_cols(df, ["Fecha", "Marca temporal", "Timestamp"])
    col_agent = coalesce_cols(df, ["Dirección de correo electrónico", "Direccion de correo electronico", "Email"])
    col_motivo = coalesce_cols(df, ["Motivo"])
    col_motivo_aeropuerto = coalesce_cols(df, ["Si es compensación Aeropuerto selecciona el motivo", "Si es compensacion Aeropuerto selecciona el motivo"])
    col_correo_cliente = coalesce_cols(df, ["Correo"])
    col_ticket = coalesce_cols(df, ["Ticket"])

    # Montos: por duplicados, preferimos "Monto.1" si existe
    col_monto = None
    # Primero intenta el duplicado (Monto.1)
    for c in df.columns:
        if str(c).strip().lower() == "monto.1":
            col_monto = c
            break
    if col_monto is None:
        col_monto = coalesce_cols(df, ["Monto"])

    missing = [("Fecha", col_fecha), ("Email agente", col_agent), ("Motivo", col_motivo),
               ("Motivo Aeropuerto", col_motivo_aeropuerto), ("Correo", col_correo_cliente),
               ("Ticket", col_ticket), ("Monto", col_monto)]
    miss_names = [n for n, c in missing if c is None]
    if miss_names:
        raise ValueError(f"[Transferencia] No pude identificar estas columnas: {', '.join(miss_names)}. Revisa encabezados del archivo.")

    # Filtrar motivo = Compensación Aeropuerto
    df_f = df[df[col_motivo].astype(str).str.strip().str.lower() == "compensación aeropuerto".lower()].copy()

    out = pd.DataFrame()
    out["Fecha"] = pd.to_datetime(df_f[col_fecha], errors="coerce", dayfirst=True)
    out["Dirección de correo electrónico"] = df_f[col_agent].map(safe_strip)
    out["Numero"] = df_f[col_ticket].map(normalize_ticket)
    out["Correo registrado en Cabify para realizar la carga"] = df_f[col_correo_cliente].map(safe_strip)  # lo usamos como "correo" cliente
    out["Monto_transferencia"] = df_f[col_monto].map(parse_amount)
    # Unificar motivo aeropuerto hacia "Motivo compensación"
    out["Motivo_transferencia"] = df_f[col_motivo_aeropuerto].map(safe_strip)

    out["Fuente_transferencia"] = True
    return out


# =========================
# Unificación / Master
# =========================
def build_master(df_saldo: pd.DataFrame, df_transf: pd.DataFrame) -> pd.DataFrame:
    # Asegurar columnas mínimas
    for c in ["Numero"]:
        if c not in df_saldo.columns or c not in df_transf.columns:
            raise ValueError("Falta columna Numero luego de transformar.")

    # Merge outer por ticket
    m = pd.merge(
        df_saldo,
        df_transf,
        on="Numero",
        how="outer",
        suffixes=("_saldo", "_transf"),
    )

    # Fecha: tomar mínima (más antigua) entre ambas
    m["Fecha"] = pd.to_datetime(m[["Fecha_saldo", "Fecha_transf"]].min(axis=1), errors="coerce")

    # Email agente: prioriza saldo si existe, si no transferencia
    m["Dirección de correo electrónico"] = m["Dirección de correo electrónico_saldo"].combine_first(
        m["Dirección de correo electrónico_transf"]
    )

    # Correo registrado para carga: prioriza saldo (que explícitamente es "correo registrado en Cabify para realizar la carga"),
    # si no, usa el Correo de transferencias (cliente)
    m["Correo registrado en Cabify para realizar la carga"] = m["Correo registrado en Cabify para realizar la carga_saldo"].combine_first(
        m["Correo registrado en Cabify para realizar la carga_transf"]
    )

    # Motivo: unir únicos (saldo + transferencia)
    def join_unique(a, b):
        vals = []
        for x in [a, b]:
            x = safe_strip(x)
            if x:
                vals.append(x)
        # unique preserving order
        out = []
        for v in vals:
            if v not in out:
                out.append(v)
        return " | ".join(out) if out else None

    m["Motivo compensación"] = [
        join_unique(a, b) for a, b in zip(m.get("Motivo_saldo"), m.get("Motivo_transferencia"))
    ]

    # Monto total
    m["Monto a compensar"] = (
        pd.to_numeric(m.get("Monto_saldo"), errors="coerce").fillna(0)
        + pd.to_numeric(m.get("Monto_transferencia"), errors="coerce").fillna(0)
    )
    m.loc[m["Monto a compensar"] == 0, "Monto a compensar"] = np.nan

    # Clasificación
    has_s = m.get("Fuente_saldo").fillna(False)
    has_t = m.get("Fuente_transferencia").fillna(False)

    def classify(s, t):
        if s and t:
            return "Aeropuerto - Mixta (Saldo + Transferencia)"
        if s:
            return "Aeropuerto - Saldo"
        if t:
            return "Aeropuerto - Transferencia"
        return None

    m["Clasificación"] = [classify(bool(s), bool(t)) for s, t in zip(has_s, has_t)]

    # id_reserva vacío por ahora
    m["id_reserva"] = ""

    # Selección final
    out = m[[
        "Fecha",
        "Dirección de correo electrónico",
        "Numero",
        "Correo registrado en Cabify para realizar la carga",
        "Monto a compensar",
        "Motivo compensación",
        "id_reserva",
        "Clasificación",
    ]].copy()

    # Ordenar por fecha desc
    out = out.sort_values(["Fecha", "Numero"], ascending=[False, False])

    return out


def df_to_excel_bytes(df: pd.DataFrame, sheet_name="master") -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return bio.getvalue()


# =========================
# UI
# =========================
st.title("Máster de Compensaciones (Aeropuerto)")

with st.sidebar:
    st.header("1) Descarga desde Google Sheets")

    st.caption("Carga de Saldo (Compensaciones por saldo)")
    saldo_export_url = build_export_xlsx_url(SHEET_SALDO_URL)
    st.link_button("Abrir Sheet (Saldo)", SHEET_SALDO_URL)

    st.caption("Compensaciones Transferencia (filtraremos Motivo = Compensación Aeropuerto)")
    transf_export_url = build_export_xlsx_url(SHEET_TRANSF_URL)
    st.link_button("Abrir Sheet (Transferencia)", SHEET_TRANSF_URL)

    st.divider()

    # Descargar bytes y generar botones
    if st.button("🔄 Descargar ambos Sheets (xlsx)"):
        st.session_state["saldo_bytes"] = fetch_bytes(saldo_export_url)
        st.session_state["transf_bytes"] = fetch_bytes(transf_export_url)
        st.session_state["saldo_sha"] = sha256_bytes(st.session_state["saldo_bytes"])
        st.session_state["transf_sha"] = sha256_bytes(st.session_state["transf_bytes"])
        st.success("Descargados. Ya puedes bajar los archivos o compararlos con los locales.")

    col1, col2 = st.columns(2)
    with col1:
        if "saldo_bytes" in st.session_state:
            st.download_button(
                "⬇️ Descargar Saldo.xlsx",
                data=st.session_state["saldo_bytes"],
                file_name="Carga_de_Saldo.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
    with col2:
        if "transf_bytes" in st.session_state:
            st.download_button(
                "⬇️ Descargar Transferencia.xlsx",
                data=st.session_state["transf_bytes"],
                file_name="Compensaciones_Transferencia.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    if "saldo_sha" in st.session_state or "transf_sha" in st.session_state:
        with st.expander("Ver checksums (SHA-256) descargados"):
            if "saldo_sha" in st.session_state:
                st.code(f"Saldo SHA-256: {st.session_state['saldo_sha']}")
            if "transf_sha" in st.session_state:
                st.code(f"Transferencia SHA-256: {st.session_state['transf_sha']}")

st.header("2) Cargar archivos locales y validar (checksum)")

cA, cB = st.columns(2)

with cA:
    up_saldo = st.file_uploader("Sube el Excel local de Carga de Saldo (xlsx)", type=["xlsx"], key="up_saldo")
with cB:
    up_transf = st.file_uploader("Sube el Excel local de Transferencias (xlsx)", type=["xlsx"], key="up_transf")

# Comparación checksum
def checksum_status(label: str, uploaded_file, session_key_bytes: str, session_key_sha: str):
    if uploaded_file is None:
        return
    ub = uploaded_file.getvalue()
    u_sha = sha256_bytes(ub)

    st.subheader(f"Checksum: {label}")
    st.write(f"Local SHA-256: `{u_sha}`")

    if session_key_sha in st.session_state:
        st.write(f"Sheet SHA-256: `{st.session_state[session_key_sha]}`")
        if u_sha == st.session_state[session_key_sha]:
            st.success("✅ El archivo local coincide con el Sheet (checksum igual).")
        else:
            st.warning("⚠️ El archivo local NO coincide con el Sheet (checksum distinto).")
    else:
        st.info("Descarga primero desde el Sheet (botón en la barra lateral) para poder comparar checksums.")

with st.container():
    c1, c2 = st.columns(2)
    with c1:
        if up_saldo is not None:
            checksum_status("Saldo", up_saldo, "saldo_bytes", "saldo_sha")
    with c2:
        if up_transf is not None:
            checksum_status("Transferencia", up_transf, "transf_bytes", "transf_sha")

st.divider()
st.header("3) Generar Máster unificado (ticket único)")

btn_run = st.button("⚙️ Generar Máster", type="primary")

if btn_run:
    if up_saldo is None or up_transf is None:
        st.error("Debes subir ambos archivos locales (Saldo y Transferencia) para generar el máster.")
        st.stop()

    # Leer
    try:
        df_saldo_raw = pd.read_excel(io.BytesIO(up_saldo.getvalue()), engine="openpyxl")
        df_transf_raw = pd.read_excel(io.BytesIO(up_transf.getvalue()), engine="openpyxl")
    except Exception as e:
        st.exception(e)
        st.stop()

    # Transformar
    try:
        df_saldo = transform_saldo(df_saldo_raw)
        df_transf = transform_transfer(df_transf_raw)
    except Exception as e:
        st.error(str(e))
        st.stop()

    # Debug info
    c1, c2, c3 = st.columns(3)
    c1.metric("Registros Saldo (raw)", len(df_saldo_raw))
    c2.metric("Registros Transfer (raw)", len(df_transf_raw))
    c3.metric("Transfer filtradas (Aeropuerto)", len(df_transf))

    # Unificar
    df_master = build_master(df_saldo, df_transf)

    # Mostrar
    st.subheader("Vista previa (máster)")
    st.dataframe(df_master, use_container_width=True, height=420)

    # Descargas
    excel_bytes = df_to_excel_bytes(df_master, sheet_name="master_compensaciones")
    csv_bytes = df_master.to_csv(index=False).encode("utf-8")

    cA, cB = st.columns(2)
    with cA:
        st.download_button(
            "⬇️ Descargar Máster (Excel)",
            data=excel_bytes,
            file_name="master_compensaciones.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    with cB:
        st.download_button(
            "⬇️ Descargar Máster (CSV)",
            data=csv_bytes,
            file_name="master_compensaciones.csv",
            mime="text/csv",
        )

    st.caption(
        "Notas: "
        "1) Transferencias se filtran por Motivo='Compensación Aeropuerto'. "
        "2) 'Motivo compensación' unifica motivo de saldo y el motivo seleccionado en transferencias. "
        "3) Monto total suma Saldo + Transferencia si el ticket aparece en ambas."
    )

st.divider()
with st.expander("Ayuda / supuestos importantes"):
    st.markdown(
        """
- **Ticket**: se normaliza a solo dígitos. Si viene en URL, se toma lo que está después del último `/`.
- **Monto**: se intenta normalizar a CLP.  
  - Si viene como `14.968` se interpreta como `14968`.  
  - Si viene como `28.471` (a veces visible como `$28.47`) se interpreta como `28471` (heurística *x1000*).
- Si tus archivos exportados traen encabezados distintos, el error te dirá qué columna no pudo identificar.
        """
    )
