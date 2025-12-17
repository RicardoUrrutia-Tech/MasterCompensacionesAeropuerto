import io
import re
import hashlib
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import requests
import streamlit as st


# =========================
# Config
# =========================
st.set_page_config(
    page_title="Máster Compensaciones Aeropuerto",
    layout="wide",
)

SHEET_SALDO_URL = "https://docs.google.com/spreadsheets/d/1Yj8q2dnlKqIZ1-vdr7wZZp_jvXiLoYO6Qwc8NeIeUnE/edit?gid=1139202449#gid=1139202449"
SHEET_TRANSFER_URL = "https://docs.google.com/spreadsheets/d/1yHTfTOD-N8VYBSzQRCkaNpMpAQHykBzVB5mYsXS6rHs/edit?resourcekey=&gid=1627777729#gid=1627777729"

SALDO_SHEET_ID = "1Yj8q2dnlKqIZ1-vdr7wZZp_jvXiLoYO6Qwc8NeIeUnE"
SALDO_GID = "1139202449"

TRANSFER_SHEET_ID = "1yHTfTOD-N8VYBSzQRCkaNpMpAQHykBzVB5mYsXS6rHs"
TRANSFER_GID = "1627777729"


# =========================
# Helpers: network + checksum
# =========================
def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def make_export_url(sheet_id: str, gid: str, fmt: str = "csv") -> str:
    # fmt: csv o xlsx
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format={fmt}&gid={gid}"


def fetch_bytes(url: str, timeout: int = 30) -> bytes:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.content


# =========================
# Helpers: reading files
# =========================
def _dedupe_columns(cols) -> list[str]:
    """
    Asegura nombres únicos. Si hay duplicados, agrega sufijos __2, __3, etc.
    """
    seen = {}
    out = []
    for c in cols:
        c = str(c).strip()
        if c not in seen:
            seen[c] = 1
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}__{seen[c]}")
    return out


def read_table_from_bytes(filename: str, b: bytes) -> pd.DataFrame:
    """
    Lee CSV/XLSX desde bytes. Devuelve df con columnas deduplicadas.
    """
    name = (filename or "").lower().strip()
    bio = io.BytesIO(b)

    if name.endswith(".xlsx") or name.endswith(".xls"):
        df = pd.read_excel(bio, engine="openpyxl")
    else:
        # Por defecto: CSV (Sheets export CSV)
        df = pd.read_csv(bio)

    df.columns = _dedupe_columns(df.columns)
    return df


def read_table_from_uploaded(uploaded) -> Tuple[pd.DataFrame, bytes]:
    b = uploaded.getvalue()
    df = read_table_from_bytes(uploaded.name, b)
    return df, b


# =========================
# Helpers: robust column access (fix ValueError: 2)
# =========================
def get_col_series(df: pd.DataFrame, col: str) -> Optional[pd.Series]:
    """
    Devuelve una Series incluso si el nombre original estaba duplicado y pandas lo dejó como DataFrame 2D.
    - Si existe col exacto: df[col]
    - Si no existe, busca case-insensitive por startswith / contains.
    - Si df[col] resultara 2D (por duplicados previos), toma la primera columna.
    """
    if df is None or df.empty:
        return None

    cols = list(df.columns)

    def _as_series(x):
        if x is None:
            return None
        if isinstance(x, pd.DataFrame):
            # <- esto es lo que dispara ValueError: 2 en tu stacktrace (2D)
            return x.iloc[:, 0]
        return x

    if col in cols:
        return _as_series(df[col])

    # case-insensitive match
    lower_map = {c.lower(): c for c in cols}
    if col.lower() in lower_map:
        return _as_series(df[lower_map[col.lower()]])

    # fuzzy search
    target = col.lower()
    candidates = [c for c in cols if target in c.lower()]
    if candidates:
        return _as_series(df[candidates[0]])

    return None


# =========================
# Parsing: tickets, amounts, ids
# =========================
TICKET_DIGITS_RE = re.compile(r"(\d{6,})")  # Zendesk suele ser 8 dígitos, pero permitimos 6+


def extract_ticket(raw) -> str:
    """
    Soporta:
    - '66171869'
    - '#66185638'
    - 'https://cabify.zendesk.com/agent/tickets/67587605' (toma lo que va después del último / o el primer bloque de dígitos largo)
    """
    if pd.isna(raw):
        return ""
    s = str(raw).strip()

    # Si viene URL, intenta extraer último segmento
    if "http://" in s or "https://" in s:
        # último tramo tras /
        last = s.rstrip("/").split("/")[-1]
        m = TICKET_DIGITS_RE.search(last)
        if m:
            return m.group(1)
        # fallback: cualquier bloque grande de dígitos
        m = TICKET_DIGITS_RE.search(s)
        return m.group(1) if m else ""

    # Si viene con '#'
    s = s.replace("#", "").strip()

    m = TICKET_DIGITS_RE.search(s)
    return m.group(1) if m else ""


UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)


def extract_id_reserva(raw) -> str:
    """
    Del campo "Link payments, link del viaje o numero reserva":
    - Si trae UUID (ej price-correction/...UUID...), devuelve ese UUID
    - Si trae un número largo en URL o texto, devuelve ese número
    - Si no, devuelve texto limpio corto
    """
    if pd.isna(raw):
        return ""
    s = str(raw).strip()
    if not s:
        return ""

    m = UUID_RE.search(s)
    if m:
        return m.group(0)

    m2 = TICKET_DIGITS_RE.search(s)
    if m2:
        return m2.group(1)

    # fallback: último segmento de URL o string truncado
    if "http://" in s or "https://" in s:
        return s.rstrip("/").split("/")[-1][:80]
    return s[:80]


def parse_amount_to_int(x) -> int:
    """
    Convierte montos con formatos:
    - 19980
    - 7.000 / 497.572 / 14.968  (puntos como miles)
    - $28.47 (caso raro: a veces Sheets lo muestra con 2 decimales aunque el real sea ~28.470+)
    - 10,990 (si apareciera)
    Devuelve pesos como entero.
    """
    if pd.isna(x):
        return 0
    s = str(x).strip()
    if s == "":
        return 0

    original = s
    s = s.replace("$", "").replace("CLP", "").replace(" ", "").strip()

    # Si tiene coma y punto, asumimos coma decimal (estilo 1.234,56)
    if "," in s and "." in s:
        s2 = s.replace(".", "").replace(",", ".")
        try:
            return int(round(float(s2)))
        except Exception:
            return 0

    # Si tiene solo coma, asumimos coma como miles o decimal según patrón
    if "," in s and "." not in s:
        # si hay 2 dígitos al final, podría ser decimal; en compensaciones no se usa -> redondeamos
        if re.search(r",\d{2}$", s):
            try:
                return int(round(float(s.replace(",", "."))))
            except Exception:
                return 0
        # si no, es miles
        s = s.replace(",", "")

    # Si tiene puntos: en CL normalmente son miles.
    # - Si hay un punto con 3 dígitos al final -> miles (28.471 => 28471)
    if re.search(r"\.\d{3}$", s):
        s = s.replace(".", "")
        try:
            return int(s)
        except Exception:
            return 0

    # - Si hay más de un punto -> miles
    if s.count(".") > 1:
        s = s.replace(".", "")
        try:
            return int(s)
        except Exception:
            return 0

    # - Si hay un punto con 2 dígitos al final (28.47):
    #   normalmente sería decimal, pero aquí suele ser formateo raro de Sheets.
    if re.search(r"\.\d{2}$", s):
        try:
            val = float(s)
        except Exception:
            return 0

        # Heurística: si es muy pequeño para ser compensación típica y venía con '$', multiplica por 1000.
        # 28.47 -> 28470 (muy cercano a 28471 esperado)
        if val < 1000 and "$" in original:
            return int(round(val * 1000))
        return int(round(val))

    # Caso general: número entero ya limpio
    s = s.replace(".", "")
    try:
        return int(s)
    except Exception:
        try:
            return int(round(float(s)))
        except Exception:
            return 0


def to_datetime_series(s: Optional[pd.Series]) -> pd.Series:
    if s is None:
        return pd.to_datetime(pd.Series([], dtype="object"), errors="coerce")

    # Maneja fecha/hora estilo "11/12/2025 16:10:00"
    out = pd.to_datetime(s, dayfirst=True, errors="coerce")

    # Si todo quedó NaT, intenta sin dayfirst (por si algún export cambia)
    if out.notna().sum() == 0:
        out = pd.to_datetime(s, errors="coerce")

    return out


# =========================
# Build master
# =========================
def build_master(df_saldo_raw: pd.DataFrame, df_transfer_raw: pd.DataFrame) -> pd.DataFrame:
    # -------- Saldo (compensaciones con cargo de saldo) --------
    # Esperado: Marca temporal / Dirección de correo electrónico / Numero ticket / Correo registrado ... / Monto / Motivo
    saldo_date = get_col_series(df_saldo_raw, "Marca temporal")
    saldo_mail = get_col_series(df_saldo_raw, "Dirección de correo electrónico")
    saldo_ticket = get_col_series(df_saldo_raw, "Numero ticket")
    saldo_cabify_mail = get_col_series(df_saldo_raw, "Correo registrado en Cabify para realizar la carga")
    saldo_amount = get_col_series(df_saldo_raw, "Monto a compensar")
    saldo_reason = get_col_series(df_saldo_raw, "Motivo compensación")

    df_saldo = pd.DataFrame({
        "Fecha": to_datetime_series(saldo_date),
        "Dirección de correo electrónico": (saldo_mail.fillna("").astype(str).str.strip() if saldo_mail is not None else ""),
        "Numero": (saldo_ticket.fillna("").map(extract_ticket) if saldo_ticket is not None else ""),
        "Correo registrado en Cabify para realizar la carga": (saldo_cabify_mail.fillna("").astype(str).str.strip() if saldo_cabify_mail is not None else ""),
        "Monto_saldo": (saldo_amount.map(parse_amount_to_int) if saldo_amount is not None else 0),
        "Motivo_saldo": (saldo_reason.fillna("").astype(str).str.strip() if saldo_reason is not None else ""),
        "Clasificación_saldo": "Aeropuerto - Saldo",
    })

    df_saldo = df_saldo[df_saldo["Numero"].astype(str).str.len() > 0].copy()

    # -------- Transferencias (carga de saldo mediante transferencia) --------
    # Filtrar Motivo == "Compensación Aeropuerto"
    transfer_date = get_col_series(df_transfer_raw, "Marca temporal")
    transfer_mail = get_col_series(df_transfer_raw, "Dirección de correo electrónico")
    transfer_ticket = get_col_series(df_transfer_raw, "Ticket")
    transfer_cabify_mail = get_col_series(df_transfer_raw, "Correo")
    transfer_amount = get_col_series(df_transfer_raw, "Monto")  # hay más de un Monto en ese sheet; tomamos el primero hallado
    transfer_motivo = get_col_series(df_transfer_raw, "Motivo")
    transfer_airport_reason = get_col_series(df_transfer_raw, "Si es compensación Aeropuerto selecciona el motivo")
    transfer_reason2 = get_col_series(df_transfer_raw, "Motivo de compensación")
    transfer_id_reserva = get_col_series(df_transfer_raw, "Link payments, link del viaje o numero reserva")

    df_transfer = pd.DataFrame({
        "Fecha": to_datetime_series(transfer_date),
        "Dirección de correo electrónico": (transfer_mail.fillna("").astype(str).str.strip() if transfer_mail is not None else ""),
        "Numero": (transfer_ticket.fillna("").map(extract_ticket) if transfer_ticket is not None else ""),
        "Correo registrado en Cabify para realizar la carga": (transfer_cabify_mail.fillna("").astype(str).str.strip() if transfer_cabify_mail is not None else ""),
        "Monto_transfer": (transfer_amount.map(parse_amount_to_int) if transfer_amount is not None else 0),
        "Motivo_transfer_base": (transfer_motivo.fillna("").astype(str).str.strip() if transfer_motivo is not None else ""),
        "Motivo_transfer_airport": (transfer_airport_reason.fillna("").astype(str).str.strip() if transfer_airport_reason is not None else ""),
        "Motivo_transfer_alt": (transfer_reason2.fillna("").astype(str).str.strip() if transfer_reason2 is not None else ""),
        "id_reserva": (transfer_id_reserva.fillna("").map(extract_id_reserva) if transfer_id_reserva is not None else ""),
        "Clasificación_transfer": "Aeropuerto - Transferencia",
    })

    df_transfer = df_transfer[df_transfer["Numero"].astype(str).str.len() > 0].copy()

    # Filtro Motivo == Compensación Aeropuerto
    df_transfer_f = df_transfer[df_transfer["Motivo_transfer_base"].str.lower() == "compensación aeropuerto".lower()].copy()

    # Unificar motivo: preferir "Si es compensación Aeropuerto selecciona el motivo", si no, usar "Motivo de compensación"
    df_transfer_f["Motivo_transfer_final"] = df_transfer_f["Motivo_transfer_airport"]
    df_transfer_f.loc[df_transfer_f["Motivo_transfer_final"].eq(""), "Motivo_transfer_final"] = df_transfer_f["Motivo_transfer_alt"]

    # -------- Merge master por ticket (Numero) --------
    # Agregamos por ticket para consolidar duplicados internos
    saldo_agg = (
        df_saldo
        .groupby("Numero", as_index=False)
        .agg({
            "Fecha": "min",
            "Dirección de correo electrónico": "first",
            "Correo registrado en Cabify para realizar la carga": "first",
            "Monto_saldo": "sum",
            "Motivo_saldo": lambda x: " | ".join([v for v in pd.unique(x) if str(v).strip() != ""]),
            "Clasificación_saldo": "first",
        })
    )

    transfer_agg = (
        df_transfer_f
        .groupby("Numero", as_index=False)
        .agg({
            "Fecha": "min",
            "Dirección de correo electrónico": "first",
            "Correo registrado en Cabify para realizar la carga": "first",
            "Monto_transfer": "sum",
            "Motivo_transfer_final": lambda x: " | ".join([v for v in pd.unique(x) if str(v).strip() != ""]),
            "id_reserva": lambda x: " | ".join([v for v in pd.unique(x) if str(v).strip() != ""]),
            "Clasificación_transfer": "first",
        })
    )

    master = pd.merge(saldo_agg, transfer_agg, on="Numero", how="outer", suffixes=("_saldo", "_transfer"))

    # Consolidar campos finales
    master["Fecha_final"] = master["Fecha_saldo"].combine_first(master["Fecha_transfer"])
    master["Dirección de correo electrónico_final"] = master["Dirección de correo electrónico_saldo"].combine_first(master["Dirección de correo electrónico_transfer"])
    master["Correo registrado en Cabify para realizar la carga_final"] = master["Correo registrado en Cabify para realizar la carga_saldo"].combine_first(
        master["Correo registrado en Cabify para realizar la carga_transfer"]
    )

    master["Monto a compensar"] = master["Monto_saldo"].fillna(0).astype(int) + master["Monto_transfer"].fillna(0).astype(int)

    # Motivo compensación: preferir saldo si existe, si no transfer
    master["Motivo compensación"] = master["Motivo_saldo"].fillna("")
    master.loc[master["Motivo compensación"].eq(""), "Motivo compensación"] = master["Motivo_transfer_final"].fillna("")

    # id_reserva: viene de transferencias
    if "id_reserva" not in master.columns:
        master["id_reserva"] = ""
    master["id_reserva"] = master["id_reserva"].fillna("")

    # Clasificación: puede ser saldo, transferencia o ambas
    def classify(row):
        has_saldo = int(row.get("Monto_saldo", 0) or 0) > 0
        has_transfer = int(row.get("Monto_transfer", 0) or 0) > 0
        if has_saldo and has_transfer:
            return "Aeropuerto - Saldo + Transferencia"
        if has_saldo:
            return "Aeropuerto - Saldo"
        if has_transfer:
            return "Aeropuerto - Transferencia"
        return ""

    master["Clasificación"] = master.apply(classify, axis=1)

    # Selección final de columnas
    out = master.rename(columns={
        "Fecha_final": "Fecha",
        "Dirección de correo electrónico_final": "Dirección de correo electrónico",
        "Correo registrado en Cabify para realizar la carga_final": "Correo registrado en Cabify para realizar la carga",
    })[
        [
            "Fecha",
            "Dirección de correo electrónico",
            "Numero",
            "Correo registrado en Cabify para realizar la carga",
            "Monto a compensar",
            "Motivo compensación",
            "id_reserva",
            "Clasificación",
        ]
    ].copy()

    out = out.sort_values(["Fecha", "Numero"], ascending=[False, True])
    return out


def to_excel_bytes(df: pd.DataFrame, sheet_name: str = "master") -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return bio.getvalue()


# =========================
# UI
# =========================
st.title("Máster de Compensaciones Aeropuerto")

with st.expander("Links de referencia (Sheets)", expanded=False):
    st.write("**Carga de Saldo (Sheet):**", SHEET_SALDO_URL)
    st.write("**Compensaciones Transferencia (Sheet):**", SHEET_TRANSFER_URL)

st.markdown(
    """
**Flujo recomendado:**
1) Descarga los CSV desde los botones (o carga tus archivos locales).  
2) La app valida checksum simple (si descargaste aquí mismo) y genera el máster por **ticket**.  
3) Descarga el máster en Excel.
"""
)

colA, colB = st.columns(2, gap="large")

# -------- Panel Saldo --------
with colA:
    st.subheader("1) Carga de Saldo (Aeropuerto)")
    saldo_export_url = make_export_url(SALDO_SHEET_ID, SALDO_GID, fmt="csv")

    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("⬇️ Descargar CSV desde Sheets (Saldo)", use_container_width=True):
            try:
                b = fetch_bytes(saldo_export_url)
                st.session_state["saldo_bytes"] = b
                st.session_state["saldo_sha"] = sha256_bytes(b)
                st.success("Descarga OK (Saldo).")
            except requests.exceptions.HTTPError as e:
                st.error(
                    "No pude descargar el CSV desde Google Sheets (Saldo). "
                    "Esto suele pasar si el Sheet no está público o tu app no tiene acceso.\n\n"
                    "Sugerencia: comparte el Sheet como **'Cualquier persona con el enlace: Lector'**, "
                    "o descarga manualmente el CSV y súbelo abajo.\n\n"
                    f"Detalle: {e}"
                )
            except Exception as e:
                st.error(f"Error inesperado descargando Saldo: {e}")

    with c2:
        # botón de download solo si existe saldo_bytes
        if "saldo_bytes" in st.session_state:
            st.download_button(
                "💾 Guardar CSV (Saldo) descargado",
                data=st.session_state["saldo_bytes"],
                file_name="carga_saldo.csv",
                mime="text/csv",
                use_container_width=True,
            )

    st.caption("O carga el archivo local (CSV o XLSX):")
    up_saldo = st.file_uploader("Subir Saldo", type=["csv", "xlsx", "xls"], key="up_saldo")

    df_saldo_raw = None
    if up_saldo is not None:
        df_saldo_raw, up_bytes = read_table_from_uploaded(up_saldo)
        up_sha = sha256_bytes(up_bytes)

        if "saldo_sha" in st.session_state:
            if up_sha != st.session_state["saldo_sha"]:
                st.warning("⚠️ El archivo local **NO coincide** con el descargado desde Sheets (checksum distinto).")
            else:
                st.success("✅ Archivo local coincide con el CSV descargado (checksum OK).")

        st.write("Vista previa (Saldo):")
        st.dataframe(df_saldo_raw.head(20), use_container_width=True)

# -------- Panel Transfer --------
with colB:
    st.subheader("2) Compensaciones por Transferencia (filtra Motivo = Compensación Aeropuerto)")
    transfer_export_url = make_export_url(TRANSFER_SHEET_ID, TRANSFER_GID, fmt="csv")

    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("⬇️ Descargar CSV desde Sheets (Transfer)", use_container_width=True):
            try:
                b = fetch_bytes(transfer_export_url)
                st.session_state["transfer_bytes"] = b
                st.session_state["transfer_sha"] = sha256_bytes(b)
                st.success("Descarga OK (Transfer).")
            except requests.exceptions.HTTPError as e:
                st.error(
                    "No pude descargar el CSV desde Google Sheets (Transfer). "
                    "Esto suele pasar si el Sheet no está público o tu app no tiene acceso.\n\n"
                    "Sugerencia: comparte el Sheet como **'Cualquier persona con el enlace: Lector'**, "
                    "o descarga manualmente el CSV y súbelo abajo.\n\n"
                    f"Detalle: {e}"
                )
            except Exception as e:
                st.error(f"Error inesperado descargando Transfer: {e}")

    with c2:
        if "transfer_bytes" in st.session_state:
            st.download_button(
                "💾 Guardar CSV (Transfer) descargado",
                data=st.session_state["transfer_bytes"],
                file_name="compensaciones_transfer.csv",
                mime="text/csv",
                use_container_width=True,
            )

    st.caption("O carga el archivo local (CSV o XLSX):")
    up_transfer = st.file_uploader("Subir Transfer", type=["csv", "xlsx", "xls"], key="up_transfer")

    df_transfer_raw = None
    if up_transfer is not None:
        df_transfer_raw, up_bytes = read_table_from_uploaded(up_transfer)
        up_sha = sha256_bytes(up_bytes)

        if "transfer_sha" in st.session_state:
            if up_sha != st.session_state["transfer_sha"]:
                st.warning("⚠️ El archivo local **NO coincide** con el descargado desde Sheets (checksum distinto).")
            else:
                st.success("✅ Archivo local coincide con el CSV descargado (checksum OK).")

        st.write("Vista previa (Transfer):")
        st.dataframe(df_transfer_raw.head(20), use_container_width=True)

st.divider()
st.subheader("3) Generar Máster por Ticket")

info_cols = st.columns(3)
with info_cols[0]:
    use_downloaded_saldo = st.checkbox("Usar Saldo descargado desde Sheets", value=("saldo_bytes" in st.session_state))
with info_cols[1]:
    use_downloaded_transfer = st.checkbox("Usar Transfer descargado desde Sheets", value=("transfer_bytes" in st.session_state))
with info_cols[2]:
    st.caption("Si subiste archivo local, por defecto se usa el local. Marca estas opciones si quieres forzar lo descargado.")

# Resolver fuentes finales
final_saldo = df_saldo_raw
final_transfer = df_transfer_raw

if use_downloaded_saldo and "saldo_bytes" in st.session_state:
    try:
        final_saldo = read_table_from_bytes("saldo.csv", st.session_state["saldo_bytes"])
    except Exception as e:
        st.error(f"No pude leer el CSV descargado (Saldo): {e}")

if use_downloaded_transfer and "transfer_bytes" in st.session_state:
    try:
        final_transfer = read_table_from_bytes("transfer.csv", st.session_state["transfer_bytes"])
    except Exception as e:
        st.error(f"No pude leer el CSV descargado (Transfer): {e}")

can_build = (final_saldo is not None) and (final_transfer is not None)

if not can_build:
    st.info("Carga ambos archivos (Saldo y Transfer) o descárgalos con los botones para poder generar el máster.")
else:
    try:
        master = build_master(final_saldo, final_transfer)

        c1, c2, c3 = st.columns([1, 1, 2])
        with c1:
            st.metric("Tickets únicos", f"{master['Numero'].nunique():,}".replace(",", "."))
        with c2:
            st.metric("Registros en máster", f"{len(master):,}".replace(",", "."))
        with c3:
            st.caption("El máster consolida por **Numero (ticket)** sumando saldo+transfer cuando aplica.")

        st.write("Vista previa (Máster):")
        st.dataframe(master.head(50), use_container_width=True)

        xlsx = to_excel_bytes(master, sheet_name="master_compensaciones")
        st.download_button(
            "⬇️ Descargar Máster (Excel)",
            data=xlsx,
            file_name="master_compensaciones_aeropuerto.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    except Exception as e:
        st.error(
            "Se produjo un error construyendo el máster. "
            "Tip: revisa que las columnas esperadas existan en tus archivos.\n\n"
            f"Detalle: {type(e).__name__}: {e}"
        )

