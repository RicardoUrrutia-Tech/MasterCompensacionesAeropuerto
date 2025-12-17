import io
import re
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
    repl = {
        "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u",
        "ñ": "n", "ü": "u"
    }
    for k, v in repl.items():
        s = s.replace(k, v)
    s = re.sub(r"\s+", " ", s)
    return s


def build_export_urls(sheet_id: str, gid: str) -> dict:
    xlsx = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx&gid={gid}"
    csv = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&gid={gid}"
    return {"xlsx": xlsx, "csv": csv}


def fetch_bytes(url: str, timeout: int = 30) -> bytes:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; MasterCompensaciones/1.0)",
        "Accept": "*/*",
    }
    r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    r.raise_for_status()

    ctype = (r.headers.get("Content-Type") or "").lower()
    if "text/html" in ctype and len(r.content) > 5000:
        raise requests.HTTPError(
            "Google devolvió HTML (posible bloqueo por permisos / login). "
            "Asegura que el Sheet esté compartido como 'Cualquiera con el enlace: Lector'."
        )
    return r.content


@st.cache_data(ttl=600, show_spinner=False)
def download_sheet_bytes(sheet_id: str, gid: str) -> dict:
    urls = build_export_urls(sheet_id, gid)

    try:
        b = fetch_bytes(urls["xlsx"])
        return {"kind": "xlsx", "bytes": b, "sha256": sha256_bytes(b), "error": None, "url": urls["xlsx"]}
    except Exception as e_xlsx:
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
        df = pd.read_excel(io.BytesIO(b), engine="openpyxl")
    return df, b, sha256_bytes(b)


def coerce_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", dayfirst=True)


def extract_ticket_number(x) -> str | None:
    if pd.isna(x):
        return None
    s = str(x).strip()

    if s.startswith("http://") or s.startswith("https://"):
        last = s.rstrip("/").split("/")[-1]
        last = last.split("?")[0].strip()
        m = re.search(r"(\d+)", last)
        return m.group(1) if m else last

    s = s.replace("#", "").strip()

    m = re.search(r"(\d{5,})", s)
    if m:
        return m.group(1)

    m2 = re.search(r"(\d+)", s)
    return m2.group(1) if m2 else s


def parse_clp_amount(x) -> float:
    """
    Montos tipo:
      19980
      497.572  -> 497572
      14.968   -> 14968
      $28.47   -> heurística: 28.47 * 1000 = 28470 (caso típico de Sheet/locale)
    """
    if pd.isna(x):
        return 0.0

    if isinstance(x, (int, float)) and pd.notna(x):
        try:
            return float(x)
        except Exception:
            return 0.0

    s_raw = str(x).strip()
    if s_raw == "":
        return 0.0

    has_dollar = "$" in s_raw
    s = s_raw.replace("$", "").replace("CLP", "").replace(" ", "").strip()

    has_dot = "." in s
    has_comma = "," in s

    if has_dollar and has_dot and (not has_comma):
        parts = s.split(".")
        if len(parts) == 2 and len(parts[1]) == 2 and len(parts[0]) <= 2:
            try:
                v = float(s)
                return float(int(round(v * 1000)))
            except Exception:
                pass

    if has_dot and not has_comma:
        s2 = s.replace(".", "")
        s2 = re.sub(r"[^\d\-]", "", s2)
        try:
            return float(s2) if s2 != "" else 0.0
        except Exception:
            return 0.0

    if has_comma and not has_dot:
        s2 = s.replace(",", "")
        s2 = re.sub(r"[^\d\-]", "", s2)
        try:
            return float(s2) if s2 != "" else 0.0
        except Exception:
            return 0.0

    if has_dot and has_comma:
        last_dot = s.rfind(".")
        last_comma = s.rfind(",")
        if last_comma > last_dot:
            s2 = s.replace(".", "").replace(",", ".")
            s2 = re.sub(r"[^\d\.\-]", "", s2)
        else:
            s2 = s.replace(",", "")
            s2 = re.sub(r"[^\d\.\-]", "", s2)
        try:
            return float(s2) if s2 != "" else 0.0
        except Exception:
            return 0.0

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


def pick_first_like(df: pd.DataFrame, base_name: str) -> str | None:
    """
    Si en Excel hay columnas duplicadas, pandas suele crear 'Monto' y 'Monto.1'.
    Esto busca base_name exacto o variantes con .1/.2 etc.
    """
    base_norm = normalize_text(base_name)
    for c in df.columns:
        cn = normalize_text(c)
        if cn == base_norm:
            return c
    # fallback: startswith
    for c in df.columns:
        cn = normalize_text(c)
        if cn.startswith(base_norm + "."):
            return c
    return None


def join_unique(a, b) -> str:
    """
    Une dos valores (posibles NaN) en un solo string, sin duplicados.
    """
    vals = []
    for v in [a, b]:
        if v is None:
            continue
        if isinstance(v, float) and pd.isna(v):
            continue
        s = str(v).strip()
        if s == "" or s.lower() == "nan":
            continue
        vals.append(s)
    # unique preserving order
    out = []
    seen = set()
    for s in vals:
        key = s.lower()
        if key not in seen:
            seen.add(key)
            out.append(s)
    return " | ".join(out) if out else ""


def agg_unique_join(series: pd.Series) -> str:
    vals = []
    for v in series.dropna().tolist():
        s = str(v).strip()
        if s == "" or s.lower() == "nan":
            continue
        vals.append(s)
    out = []
    seen = set()
    for s in vals:
        key = s.lower()
        if key not in seen:
            seen.add(key)
            out.append(s)
    return " | ".join(out) if out else ""


def first_non_empty(series: pd.Series) -> str:
    for v in series.dropna().tolist():
        s = str(v).strip()
        if s != "" and s.lower() != "nan":
            return s
    return ""


def standardize_saldo(df: pd.DataFrame) -> pd.DataFrame:
    c_fecha = pick_col(df, ["Marca temporal", "Timestamp", "Fecha", "fecha"])
    c_mail_agente = pick_col(df, ["Dirección de correo electrónico", "Direccion de correo electronico", "Email", "Agente"])
    c_ticket = pick_col(df, ["Numero ticket", "Número ticket", "Nº ticket", "Ticket", "Numero de ticket"])
    c_mail_carga = pick_col(df, ["Correo registrado en Cabify para realizar la carga", "Correo registrado", "Correo", "Mail"])
    c_monto = pick_col(df, ["Monto a compensar", "Monto", "Importe"])
    c_motivo = pick_col(df, ["Motivo compensación", "Motivo compensacion", "Motivo"])

    out = pd.DataFrame()
    out["Fecha"] = coerce_datetime(df[c_fecha]) if c_fecha else pd.NaT
    out["Agente_saldo"] = df[c_mail_agente] if c_mail_agente else ""
    out["Numero"] = df[c_ticket].apply(extract_ticket_number) if c_ticket else None
    out["Cliente_saldo"] = df[c_mail_carga] if c_mail_carga else ""
    out["Monto_saldo"] = df[c_monto].apply(parse_clp_amount) if c_monto else 0.0
    out["Motivo_saldo"] = df[c_motivo] if c_motivo else ""
    out["Fuente_saldo"] = True
    return out


def standardize_transfer(df: pd.DataFrame) -> pd.DataFrame:
    c_fecha = pick_col(df, ["Marca temporal", "Timestamp", "Fecha", "fecha"])
    c_mail_agente = pick_col(df, ["Dirección de correo electrónico", "Direccion de correo electronico", "Email", "Agente"])
    c_motivo = pick_col(df, ["Motivo"])

    # Puede existir "Monto" duplicado (Monto, Monto.1)
    c_monto = pick_first_like(df, "Monto") or pick_col(df, ["Monto"])

    c_correo = pick_col(df, ["Correo", "Correo registrado", "Email pasajero"])
    c_ticket = pick_col(df, ["Ticket", "Numero ticket", "Número ticket", "Numero de ticket"])
    c_motivo_aerop = pick_col(df, ["Si es compensación Aeropuerto selecciona el motivo", "Si es compensacion Aeropuerto selecciona el motivo"])

    # id_reserva (tal cual viene)
    c_id_reserva = pick_col(df, ["Link payments, link del viaje o numero reserva", "Link payments, link del viaje o numero reserva ", "Link payments", "Link del viaje o numero reserva"])

    out = pd.DataFrame()
    out["Fecha"] = coerce_datetime(df[c_fecha]) if c_fecha else pd.NaT
    out["Agente_transfer"] = df[c_mail_agente] if c_mail_agente else ""
    out["Motivo"] = df[c_motivo] if c_motivo else ""
    out["Numero"] = df[c_ticket].apply(extract_ticket_number) if c_ticket else None
    out["Cliente_transfer"] = df[c_correo] if c_correo else ""
    out["Monto_transfer"] = df[c_monto].apply(parse_clp_amount) if c_monto else 0.0
    out["Motivo_transfer_aeropuerto"] = df[c_motivo_aerop] if c_motivo_aerop else ""
    out["id_reserva"] = df[c_id_reserva] if c_id_reserva else ""
    out["Fuente_transfer"] = True

    # Filtrar solo Compensación Aeropuerto
    if "Motivo" in out.columns:
        out = out[out["Motivo"].astype(str).str.strip().str.lower() == "compensación aeropuerto".lower()].copy()

    return out


def aggregate_by_ticket_saldo(s: pd.DataFrame) -> pd.DataFrame:
    s = s.copy()
    s = s[s["Numero"].notna() & (s["Numero"].astype(str).str.strip() != "")]
    # asegurar tipos
    s["Monto_saldo"] = pd.to_numeric(s["Monto_saldo"], errors="coerce").fillna(0.0)

    g = s.groupby("Numero", as_index=False).agg(
        Fecha_saldo=("Fecha", "min"),
        Agente_saldo=("Agente_saldo", agg_unique_join),
        Cliente_saldo=("Cliente_saldo", agg_unique_join),
        Monto_saldo=("Monto_saldo", "sum"),
        Motivo_saldo=("Motivo_saldo", first_non_empty),
        Fuente_saldo=("Fuente_saldo", "max"),
    )
    return g


def aggregate_by_ticket_transfer(t: pd.DataFrame) -> pd.DataFrame:
    t = t.copy()
    t = t[t["Numero"].notna() & (t["Numero"].astype(str).str.strip() != "")]
    t["Monto_transfer"] = pd.to_numeric(t["Monto_transfer"], errors="coerce").fillna(0.0)

    g = t.groupby("Numero", as_index=False).agg(
        Fecha_transfer=("Fecha", "min"),
        Agente_transfer=("Agente_transfer", agg_unique_join),
        Cliente_transfer=("Cliente_transfer", agg_unique_join),
        Monto_transfer=("Monto_transfer", "sum"),
        Motivo_transfer_aeropuerto=("Motivo_transfer_aeropuerto", first_non_empty),
        id_reserva=("id_reserva", agg_unique_join),
        Fuente_transfer=("Fuente_transfer", "max"),
    )
    return g


def build_master(df_saldo_std: pd.DataFrame, df_transf_std: pd.DataFrame) -> pd.DataFrame:
    s = aggregate_by_ticket_saldo(df_saldo_std)
    t = aggregate_by_ticket_transfer(df_transf_std)

    merged = pd.merge(s, t, on="Numero", how="outer")

    # Fecha: mínima no nula entre ambas
    f1 = pd.to_datetime(merged.get("Fecha_saldo"), errors="coerce")
    f2 = pd.to_datetime(merged.get("Fecha_transfer"), errors="coerce")
    merged["Fecha"] = f1
    merged["Fecha"] = merged["Fecha"].where(merged["Fecha"].notna(), f2)
    merged["Fecha"] = pd.concat([f1, f2], axis=1).min(axis=1)

    # Correo agente (relación saldo vs transfer): si distintos, dejar ambos
    merged["Dirección de correo electrónico"] = merged.apply(
        lambda r: join_unique(r.get("Agente_saldo", ""), r.get("Agente_transfer", "")),
        axis=1,
    )

    # Correo cliente (Correo vs Correo registrado...): si distintos, dejar ambos
    merged["Correo registrado en Cabify para realizar la carga"] = merged.apply(
        lambda r: join_unique(r.get("Cliente_saldo", ""), r.get("Cliente_transfer", "")),
        axis=1,
    )

    # Montos separados + total
    merged["Monto Saldo"] = pd.to_numeric(merged.get("Monto_saldo"), errors="coerce").fillna(0.0)
    merged["Monto Transferencia"] = pd.to_numeric(merged.get("Monto_transfer"), errors="coerce").fillna(0.0)
    merged["Total Compensación"] = merged["Monto Saldo"] + merged["Monto Transferencia"]

    # Motivo unificado: prefer saldo, si no, motivo aeropuerto de transfer
    merged["Motivo compensación"] = merged.get("Motivo_saldo", "")
    merged["Motivo compensación"] = merged["Motivo compensación"].where(
        merged["Motivo compensación"].astype(str).str.strip() != "",
        merged.get("Motivo_transfer_aeropuerto", ""),
    )

    # id_reserva (tal cual; si no existe, vacío)
    merged["id_reserva"] = merged.get("id_reserva", "")
    merged["id_reserva"] = merged["id_reserva"].fillna("").astype(str)

    # Clasificación
    has_s = merged.get("Fuente_saldo", False)
    has_t = merged.get("Fuente_transfer", False)
    has_s = has_s.fillna(False) if isinstance(has_s, pd.Series) else False
    has_t = has_t.fillna(False) if isinstance(has_t, pd.Series) else False

    merged["Clasificación"] = "Sin clasificar"
    merged.loc[has_s & ~has_t, "Clasificación"] = "Saldo (Aeropuerto)"
    merged.loc[~has_s & has_t, "Clasificación"] = "Transferencia (Aeropuerto)"
    merged.loc[has_s & has_t, "Clasificación"] = "Saldo + Transferencia (Aeropuerto)"

    final = merged[
        [
            "Fecha",
            "Dirección de correo electrónico",
            "Numero",
            "Correo registrado en Cabify para realizar la carga",
            "Monto Saldo",
            "Monto Transferencia",
            "Total Compensación",
            "Motivo compensación",
            "id_reserva",
            "Clasificación",
        ]
    ].copy()

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

    up_saldo = st.file_uploader(
        "📤 Cargar archivo local de Carga de Saldo (xlsx/csv)",
        type=["xlsx", "xls", "csv"],
        key="up_saldo",
    )

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

    up_transf = st.file_uploader(
        "📤 Cargar archivo local de Transferencias (xlsx/csv)",
        type=["xlsx", "xls", "csv"],
        key="up_transf",
    )


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
