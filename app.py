import io
import re
import json
import hashlib
from datetime import datetime

import pandas as pd
import requests
import streamlit as st


# =========================
# Config
# =========================
st.set_page_config(page_title="Máster Compensaciones Aeropuerto", layout="wide")

SALDO_SHEET_URL = "https://docs.google.com/spreadsheets/d/1Yj8q2dnlKqIZ1-vdr7wZZp_jvXiLoYO6Qwc8NeIeUnE/edit?gid=1139202449#gid=1139202449"
SALDO_SHEET_ID = "1Yj8q2dnlKqIZ1-vdr7wZZp_jvXiLoYO6Qwc8NeIeUnE"
SALDO_GID = "1139202449"

TRANSF_SHEET_URL = "https://docs.google.com/spreadsheets/d/1yHTfTOD-N8VYBSzQRCkaNpMpAQHykBzVB5mYsXS6rHs/edit?resourcekey=&gid=1627777729#gid=1627777729"
TRANSF_SHEET_ID = "1yHTfTOD-N8VYBSzQRCkaNpMpAQHykBzVB5mYsXS6rHs"
TRANSF_GID = "1627777729"


# =========================
# Helpers
# =========================
def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.strip().lower()
    # normalización simple sin dependencias extras
    repl = {
        "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u",
        "ñ": "n", "ü": "u"
    }
    for k, v in repl.items():
        s = s.replace(k, v)
    s = re.sub(r"\s+", " ", s)
    return s


def build_export_urls(sheet_id: str, gid: str) -> dict:
    # XLSX export (mejor para conservar valores numéricos reales)
    xlsx = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx&gid={gid}"
    # CSV fallback (a veces más permisivo)
    csv = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&gid={gid}"
    return {"xlsx": xlsx, "csv": csv}


def fetch_bytes(url: str, timeout: int = 30) -> bytes:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; MasterCompensaciones/1.0)",
        "Accept": "*/*",
    }
    r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    r.raise_for_status()

    # Si Google redirige a login, usualmente devuelve HTML
    ctype = (r.headers.get("Content-Type") or "").lower()
    if "text/html" in ctype and len(r.content) > 5000:
        # Heurística: export bloqueado o login
        raise requests.HTTPError(
            "Google devolvió HTML (posible bloqueo por permisos / login). "
            "Asegura que el Sheet esté compartido como 'Cualquiera con el enlace: Lector'."
        )
    return r.content


@st.cache_data(ttl=600, show_spinner=False)
def download_sheet_bytes(sheet_id: str, gid: str) -> dict:
    """
    Intenta XLSX primero, luego CSV.
    Devuelve dict con:
      - kind: 'xlsx'|'csv'
      - bytes: b'...'
      - sha256: '...'
      - error: str|None
    """
    urls = build_export_urls(sheet_id, gid)

    # intento XLSX
    try:
        b = fetch_bytes(urls["xlsx"])
        return {"kind": "xlsx", "bytes": b, "sha256": sha256_bytes(b), "error": None, "url": urls["xlsx"]}
    except Exception as e_xlsx:
        # fallback CSV
        try:
            b = fetch_bytes(urls["csv"])
            return {"kind": "csv", "bytes": b, "sha256": sha256_bytes(b), "error": None, "url": urls["csv"]}
        except Exception as e_csv:
            return {
                "kind": None,
                "bytes": None,
                "sha256": None,
                "error": f"Falló XLSX: {type(e_xlsx).__name__}: {e_xlsx} | Falló CSV: {type(e_csv).__name__}: {e_csv}",
                "url": None,
            }


def read_uploaded_file(uploaded_file) -> tuple[pd.DataFrame, bytes, str]:
    b = uploaded_file.getvalue()
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(b))
    else:
        # xlsx/xls
        df = pd.read_excel(io.BytesIO(b), engine="openpyxl")
    return df, b, sha256_bytes(b)


def coerce_datetime(series: pd.Series) -> pd.Series:
    # soporte dd/mm/yyyy hh:mm:ss (Chile) y otros
    return pd.to_datetime(series, errors="coerce", dayfirst=True)


def extract_ticket_number(x) -> str | None:
    if pd.isna(x):
        return None
    s = str(x).strip()

    # si es link, tomar lo que viene después del último "/"
    if s.startswith("http://") or s.startswith("https://"):
        last = s.rstrip("/").split("/")[-1]
        # por si trae querystring
        last = last.split("?")[0].strip()
        # quedarse con dígitos si aplica
        m = re.search(r"(\d+)", last)
        return m.group(1) if m else last

    # si viene con '#'
    s = s.replace("#", "").strip()

    # si viene con texto raro, rescatar solo dígitos largos
    m = re.search(r"(\d{5,})", s)
    if m:
        return m.group(1)

    # último recurso: dígitos cualquiera
    m2 = re.search(r"(\d+)", s)
    return m2.group(1) if m2 else s


def parse_clp_amount(x) -> float:
    """
    Intenta interpretar montos tipo:
      19980
      497.572  -> 497572
      14.968   -> 14968
      $28.47   -> heurística: 28.47 * 1000 = 28470 (caso típico de Sheet/locale)
    """
    if pd.isna(x):
        return 0.0

    # si ya es número
    if isinstance(x, (int, float)) and pd.notna(x):
        return float(x)

    s_raw = str(x).strip()
    if s_raw == "":
        return 0.0

    has_dollar = "$" in s_raw
    s = s_raw.replace("$", "").replace("CLP", "").replace(" ", "").strip()

    # Detectar separadores
    has_dot = "." in s
    has_comma = "," in s

    # Caso $28.47 (visual) -> suele significar 28.471 en barra de fórmulas (miles).
    # Heurística: si viene con $ y tiene exactamente 2 decimales y parte entera corta, multiplicar por 1000.
    if has_dollar and has_dot and (not has_comma):
        parts = s.split(".")
        if len(parts) == 2 and len(parts[1]) == 2 and len(parts[0]) <= 2:
            try:
                v = float(s)
                return float(int(round(v * 1000)))
            except Exception:
                pass

    # Si solo tiene puntos: asumir miles -> remover puntos
    if has_dot and not has_comma:
        s2 = s.replace(".", "")
        s2 = re.sub(r"[^\d\-]", "", s2)
        try:
            return float(s2) if s2 != "" else 0.0
        except Exception:
            return 0.0

    # Si solo tiene coma: a veces decimal, pero montos suelen ser enteros -> remover coma
    if has_comma and not has_dot:
        s2 = s.replace(",", "")
        s2 = re.sub(r"[^\d\-]", "", s2)
        try:
            return float(s2) if s2 != "" else 0.0
        except Exception:
            return 0.0

    # Si tiene ambos: decidir por último separador
    if has_dot and has_comma:
        last_dot = s.rfind(".")
        last_comma = s.rfind(",")
        if last_comma > last_dot:
            # coma decimal: quitar miles (.) y cambiar coma por punto
            s2 = s.replace(".", "").replace(",", ".")
            s2 = re.sub(r"[^\d\.\-]", "", s2)
        else:
            # punto decimal: quitar comas miles
            s2 = s.replace(",", "")
            s2 = re.sub(r"[^\d\.\-]", "", s2)
        try:
            return float(s2) if s2 != "" else 0.0
        except Exception:
            return 0.0

    # fallback
    s2 = re.sub(r"[^\d\-]", "", s)
    try:
        return float(s2) if s2 != "" else 0.0
    except Exception:
        return 0.0


def pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols_norm = {normalize_text(c): c for c in df.columns}
    for cand in candidates:
        c_norm = normalize_text(cand)
        if c_norm in cols_norm:
            return cols_norm[c_norm]
    return None


def standardize_saldo(df: pd.DataFrame) -> pd.DataFrame:
    # candidatos basados en tu descripción
    c_fecha = pick_col(df, ["Marca temporal", "Timestamp", "Fecha", "fecha"])
    c_mail_agente = pick_col(df, ["Dirección de correo electrónico", "Direccion de correo electronico", "Email", "Agente"])
    c_ticket = pick_col(df, ["Numero ticket", "Número ticket", "Nº ticket", "Ticket", "Numero de ticket"])
    c_mail_carga = pick_col(df, ["Correo registrado en Cabify para realizar la carga", "Correo registrado", "Correo", "Mail"])
    c_monto = pick_col(df, ["Monto a compensar", "Monto", "Importe"])
    c_motivo = pick_col(df, ["Motivo compensación", "Motivo compensacion", "Motivo"])

    out = pd.DataFrame()
    out["Fecha"] = coerce_datetime(df[c_fecha]) if c_fecha else pd.NaT
    out["Dirección de correo electrónico"] = df[c_mail_agente] if c_mail_agente else None
    out["Numero"] = df[c_ticket].apply(extract_ticket_number) if c_ticket else None
    out["Correo registrado en Cabify para realizar la carga"] = df[c_mail_carga] if c_mail_carga else None
    out["Monto_saldo"] = df[c_monto].apply(parse_clp_amount) if c_monto else 0.0
    out["Motivo_saldo"] = df[c_motivo] if c_motivo else None
    out["Fuente_saldo"] = True
    return out


def standardize_transfer(df: pd.DataFrame) -> pd.DataFrame:
    c_fecha = pick_col(df, ["Marca temporal", "Timestamp", "Fecha", "fecha"])
    c_mail_agente = pick_col(df, ["Dirección de correo electrónico", "Direccion de correo electronico", "Email", "Agente"])
    c_motivo = pick_col(df, ["Motivo"])
    c_monto = pick_col(df, ["Monto"])  # puede haber 2; tomamos el primero si existe
    c_correo = pick_col(df, ["Correo", "Correo registrado", "Email pasajero"])
    c_ticket = pick_col(df, ["Ticket", "Numero ticket", "Número ticket"])
    c_motivo_aerop = pick_col(df, ["Si es compensación Aeropuerto selecciona el motivo", "Si es compensacion Aeropuerto selecciona el motivo"])

    out = pd.DataFrame()
    out["Fecha"] = coerce_datetime(df[c_fecha]) if c_fecha else pd.NaT
    out["Dirección de correo electrónico"] = df[c_mail_agente] if c_mail_agente else None
    out["Motivo"] = df[c_motivo] if c_motivo else None
    out["Numero"] = df[c_ticket].apply(extract_ticket_number) if c_ticket else None
    out["Correo registrado en Cabify para realizar la carga"] = df[c_correo] if c_correo else None
    out["Monto_transfer"] = df[c_monto].apply(parse_clp_amount) if c_monto else 0.0
    out["Motivo_transfer_aeropuerto"] = df[c_motivo_aerop] if c_motivo_aerop else None
    out["Fuente_transfer"] = True

    # filtrar solo Compensación Aeropuerto
    if "Motivo" in out.columns and out["Motivo"].notna().any():
        out = out[out["Motivo"].astype(str).str.strip().str.lower() == "compensación aeropuerto".lower()].copy()

    return out


def build_master(df_saldo_std: pd.DataFrame, df_transf_std: pd.DataFrame) -> pd.DataFrame:
    # Agrupar por ticket (Numero)
    s = df_saldo_std.copy()
    t = df_transf_std.copy()

    # Para evitar tickets vacíos
    s = s[s["Numero"].notna() & (s["Numero"].astype(str).str.strip() != "")]
    t = t[t["Numero"].notna() & (t["Numero"].astype(str).str.strip() != "")]

    # Unir por ticket (outer)
    merged = pd.merge(
        s,
        t,
        on="Numero",
        how="outer",
        suffixes=("_saldo", "_transfer"),
    )

    # Fecha: mínima no nula entre ambas
    merged["Fecha"] = pd.to_datetime(merged["Fecha_saldo"], errors="coerce")
    f2 = pd.to_datetime(merged["Fecha_transfer"], errors="coerce")
    merged["Fecha"] = merged["Fecha"].where(merged["Fecha"].notna(), f2)
    merged["Fecha"] = merged[["Fecha", f2.name]].min(axis=1)

    # Email agente: prefer saldo
    merged["Dirección de correo electrónico"] = merged["Dirección de correo electrónico_saldo"]
    merged["Dirección de correo electrónico"] = merged["Dirección de correo electrónico"].where(
        merged["Dirección de correo electrónico"].notna(),
        merged["Dirección de correo electrónico_transfer"],
    )

    # Correo registrado: prefer saldo
    merged["Correo registrado en Cabify para realizar la carga"] = merged[
        "Correo registrado en Cabify para realizar la carga_saldo"
    ]
    merged["Correo registrado en Cabify para realizar la carga"] = merged[
        "Correo registrado en Cabify para realizar la carga"
    ].where(
        merged["Correo registrado en Cabify para realizar la carga"].notna(),
        merged["Correo registrado en Cabify para realizar la carga_transfer"],
    )

    # Monto total
    merged["Monto_saldo"] = merged["Monto_saldo"].fillna(0.0)
    merged["Monto_transfer"] = merged["Monto_transfer"].fillna(0.0)
    merged["Monto a compensar"] = merged["Monto_saldo"] + merged["Monto_transfer"]

    # Motivo unificado: prefer saldo, si no, usar motivo aeropuerto de transfer
    merged["Motivo compensación"] = merged["Motivo_saldo"]
    merged["Motivo compensación"] = merged["Motivo compensación"].where(
        merged["Motivo compensación"].notna() & (merged["Motivo compensación"].astype(str).str.strip() != ""),
        merged["Motivo_transfer_aeropuerto"],
    )

    # Clasificación
    has_s = merged["Fuente_saldo"].fillna(False)
    has_t = merged["Fuente_transfer"].fillna(False)
    merged["Clasificación"] = "Sin clasificar"
    merged.loc[has_s & ~has_t, "Clasificación"] = "Saldo (Aeropuerto)"
    merged.loc[~has_s & has_t, "Clasificación"] = "Transferencia (Aeropuerto)"
    merged.loc[has_s & has_t, "Clasificación"] = "Saldo + Transferencia (Aeropuerto)"

    # id_reserva por ahora vacío
    merged["id_reserva"] = ""

    # Selección final de columnas
    final = merged[
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

    # Ordenar por fecha desc
    final = final.sort_values("Fecha", ascending=False, na_position="last").reset_index(drop=True)
    return final


# =========================
# UI
# =========================
st.title("Máster de Compensaciones Aeropuerto")

with st.expander("Links de referencia", expanded=False):
    st.write("**Carga de Saldo**:", SALDO_SHEET_URL)
    st.write("**Compensaciones Transferencia**:", TRANSF_SHEET_URL)

colA, colB = st.columns(2)

with colA:
    st.subheader("1) Carga de Saldo (Sheet)")
    dl_saldo = download_sheet_bytes(SALDO_SHEET_ID, SALDO_GID)
    if dl_saldo["error"]:
        st.error("No pude descargar desde Google Sheets (aún puedes cargar archivo local).")
        st.caption(dl_saldo["error"])
        st.write("Prueba abrir el Sheet y compartirlo como **Cualquiera con el enlace: Lector**.")
        st.write("Link:", SALDO_SHEET_URL)
    else:
        st.success(f"Descarga lista desde Sheet ({dl_saldo['kind'].upper()}). SHA256: {dl_saldo['sha256'][:12]}…")
        st.download_button(
            label="⬇️ Descargar Carga de Saldo",
            data=dl_saldo["bytes"],
            file_name=f"carga_saldo_{SALDO_GID}.{ 'xlsx' if dl_saldo['kind']=='xlsx' else 'csv'}",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            if dl_saldo["kind"] == "xlsx"
            else "text/csv",
        )

    up_saldo = st.file_uploader("📤 Cargar archivo local de Carga de Saldo (xlsx/csv)", type=["xlsx", "xls", "csv"], key="up_saldo")

with colB:
    st.subheader("2) Transferencias (Sheet)")
    dl_transf = download_sheet_bytes(TRANSF_SHEET_ID, TRANSF_GID)
    if dl_transf["error"]:
        st.error("No pude descargar desde Google Sheets (aún puedes cargar archivo local).")
        st.caption(dl_transf["error"])
        st.write("Prueba abrir el Sheet y compartirlo como **Cualquiera con el enlace: Lector**.")
        st.write("Link:", TRANSF_SHEET_URL)
    else:
        st.success(f"Descarga lista desde Sheet ({dl_transf['kind'].upper()}). SHA256: {dl_transf['sha256'][:12]}…")
        st.download_button(
            label="⬇️ Descargar Transferencias",
            data=dl_transf["bytes"],
            file_name=f"transferencias_{TRANSF_GID}.{ 'xlsx' if dl_transf['kind']=='xlsx' else 'csv'}",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            if dl_transf["kind"] == "xlsx"
            else "text/csv",
        )

    up_transf = st.file_uploader("📤 Cargar archivo local de Transferencias (xlsx/csv)", type=["xlsx", "xls", "csv"], key="up_transf")


st.divider()
st.subheader("3) Validación (checksum) + Generación máster")

def checksum_block(label: str, uploaded_file, dl_info: dict):
    if uploaded_file is None:
        st.info(f"{label}: carga un archivo local para validar checksum.")
        return None

    df, b, sha = read_uploaded_file(uploaded_file)
    st.write(f"**{label} (local)**: `{uploaded_file.name}` — SHA256: `{sha[:12]}…`")

    if dl_info.get("sha256"):
        if sha == dl_info["sha256"]:
            st.success(f"{label}: ✅ coincide con lo último descargado del Sheet.")
        else:
            st.warning(
                f"{label}: ⚠️ NO coincide con lo último descargado del Sheet. "
                f"(local {sha[:12]}… vs sheet {dl_info['sha256'][:12]}…)"
            )
    else:
        st.caption(f"{label}: checksum contra Sheet no disponible (descarga bloqueada o fallida).")

    return df


c1, c2 = st.columns(2)
with c1:
    df_saldo_in = checksum_block("Carga de Saldo", up_saldo, dl_saldo)
with c2:
    df_transf_in = checksum_block("Transferencias", up_transf, dl_transf)

btn = st.button("⚙️ Generar Máster por Ticket", type="primary")

if btn:
    if df_saldo_in is None or df_transf_in is None:
        st.error("Necesitas cargar ambos archivos localmente para generar el máster.")
        st.stop()

    try:
        df_saldo_std = standardize_saldo(df_saldo_in)
        df_transf_std = standardize_transfer(df_transf_in)
        master = build_master(df_saldo_std, df_transf_std)

        st.success(f"Máster generado: {len(master):,} tickets únicos.")
        st.dataframe(master, use_container_width=True, height=420)

        # Export xlsx
        out_xlsx = io.BytesIO()
        with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
            master.to_excel(writer, index=False, sheet_name="Master")
            df_saldo_std.to_excel(writer, index=False, sheet_name="Saldo_std")
            df_transf_std.to_excel(writer, index=False, sheet_name="Transfer_std")
        out_xlsx.seek(0)

        st.download_button(
            "⬇️ Descargar Máster (Excel)",
            data=out_xlsx.getvalue(),
            file_name=f"master_compensaciones_aeropuerto_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except Exception as e:
        st.error("Ocurrió un error generando el máster.")
        st.exception(e)


with st.expander("Diagnóstico (por si falla la descarga desde Sheets)", expanded=False):
    st.write(
        "- Si ves **HTTPError** en Streamlit Cloud al intentar descargar, casi siempre es por permisos.\n"
        "- Verifica que cada Google Sheet esté compartido como **Cualquiera con el enlace: Lector**.\n"
        "- Si está restringido a tu organización, Google puede devolver HTML/login y la app lo bloqueará.\n"
        "- Aun así, puedes usar **carga local** y el máster se generará igual."
    )

