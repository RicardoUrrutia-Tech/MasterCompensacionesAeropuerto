import re
import io
import requests
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Máster Compensaciones (Aeropuerto)", layout="wide")

# -----------------------------
# Helpers: Google Sheets -> CSV
# -----------------------------
def parse_gsheet_id_and_gid(url: str):
    """
    Soporta URLs tipo:
    https://docs.google.com/spreadsheets/d/<ID>/edit?gid=<GID>#gid=<GID>
    """
    if not isinstance(url, str) or "spreadsheets/d/" not in url:
        return None, None

    m = re.search(r"/spreadsheets/d/([^/]+)", url)
    sheet_id = m.group(1) if m else None

    gid = None
    # intentar primero gid= en query
    m2 = re.search(r"[?&]gid=(\d+)", url)
    if m2:
        gid = m2.group(1)
    else:
        # intentar #gid=
        m3 = re.search(r"#gid=(\d+)", url)
        if m3:
            gid = m3.group(1)

    return sheet_id, gid

def gsheet_csv_export_url(url: str) -> str:
    sheet_id, gid = parse_gsheet_id_and_gid(url)
    if not sheet_id or not gid:
        raise ValueError("No pude leer el ID o el gid desde el link. Asegúrate que tenga .../d/<ID>/... y gid=<n>.")
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

def read_gsheet_csv(url: str) -> pd.DataFrame:
    export_url = gsheet_csv_export_url(url)
    r = requests.get(export_url, timeout=60)
    r.raise_for_status()
    content = r.content.decode("utf-8", errors="replace")
    # pandas lee CSV desde string
    return pd.read_csv(io.StringIO(content))

# -----------------------------
# Normalización Ticket / Monto
# -----------------------------
def normalize_ticket(raw):
    """
    - '#66185638' -> '66185638'
    - 'https://.../tickets/67587605' -> '67587605'
    - '66171869' -> '66171869'
    """
    if pd.isna(raw):
        return None
    s = str(raw).strip()
    if not s:
        return None

    # Si es URL: tomar lo que está después del último "/"
    if "http://" in s.lower() or "https://" in s.lower():
        s = s.rstrip("/")
        last = s.split("/")[-1].strip()
        # por si trae querystring
        last = last.split("?")[0].strip()
        s = last

    # remover "#"
    s = s.replace("#", "").strip()

    # dejar solo dígitos (por si vienen espacios)
    digits = re.findall(r"\d+", s)
    if not digits:
        return None
    # si hay varios grupos, concatenar suele ser mala idea; tomamos el más largo
    return max(digits, key=len)

def normalize_amount(raw):
    """
    Normaliza montos chilenos desde:
    - '497.572' -> 497572
    - '14.968' -> 14968
    - '$28.47'  -> intenta corregir caso Sheets: 28.471 mostrado como 28.47
    - '17980'   -> 17980
    """
    if pd.isna(raw):
        return 0

    s = str(raw).strip()
    if s == "" or s.lower() in {"nan", "none"}:
        return 0

    # quitar símbolos y espacios
    s = s.replace("$", "").replace("CLP", "").replace("clp", "").replace(" ", "").strip()

    # Si tiene coma, asumir coma decimal y punto miles (latam)
    # Ej: 1.234,56 -> 1234.56
    if "," in s:
        s2 = s.replace(".", "").replace(",", ".")
        try:
            return int(round(float(s2)))
        except:
            return 0

    # Sin coma:
    # Caso común Chile: puntos como separador de miles -> 497.572
    # Caso raro: '$28.47' visto como 28.471 (miles con 3 decimales) y se muestra redondeado a 2 decimales.
    # Heurística:
    # - si tiene 1 punto y termina en 3 dígitos -> miles (14.968)
    # - si tiene 1 punto y termina en 2 dígitos y el valor es "pequeño" (<1000) -> probable miles oculto -> *1000
    if re.fullmatch(r"\d+\.\d{3}", s):
        try:
            return int(s.replace(".", ""))
        except:
            return 0

    if re.fullmatch(r"\d+\.\d{2}", s):
        # heurística caso '$28.47' que representa 28.471 aprox
        try:
            val = float(s)
            if val < 1000:
                return int(round(val * 1000))  # 28.47 -> 28470 (aprox 28471)
            return int(round(val))
        except:
            return 0

    # Si tiene puntos múltiples, asumir miles (1.234.567)
    if re.fullmatch(r"\d{1,3}(\.\d{3})+", s):
        try:
            return int(s.replace(".", ""))
        except:
            return 0

    # fallback: número directo
    try:
        return int(round(float(s)))
    except:
        # último fallback: extraer dígitos
        digs = re.findall(r"\d+", s)
        return int(digs[0]) if digs else 0

def try_parse_datetime(series: pd.Series):
    # intenta día/mes/año + hora (lo que muestras)
    return pd.to_datetime(series, errors="coerce", dayfirst=True)

def uniq_join(values):
    vals = [str(v).strip() for v in values if pd.notna(v) and str(v).strip() != ""]
    if not vals:
        return ""
    # únicos conservando orden
    seen = set()
    out = []
    for v in vals:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return " | ".join(out)

# -----------------------------
# Transformación bases
# -----------------------------
def transform_saldos(df: pd.DataFrame) -> pd.DataFrame:
    # Esperado (según tu descripción):
    # Marca temporal (a veces viene sin nombre claro) + Dirección de correo electrónico + Numero ticket +
    # Correo registrado... + Monto a compensar + Motivo compensación
    df0 = df.copy()

    # Intento de encontrar columnas por nombre (tolerante)
    def pick(col_candidates):
        for c in col_candidates:
            if c in df0.columns:
                return c
        return None

    col_fecha = pick(["Marca temporal", "Fecha", "Timestamp", "Marca de tiempo"])
    col_agente = pick(["Dirección de correo electrónico", "Direccion de correo electronico", "Email", "Agente"])
    col_ticket = pick(["Numero ticket", "Número ticket", "Ticket", "Numero", "N° Ticket"])
    col_correo_carga = pick(["Correo registrado en Cabify para realizar la carga", "Correo", "Correo registrado"])
    col_monto = pick(["Monto a compensar", "Monto", "Monto compensar"])
    col_motivo = pick(["Motivo compensación", "Motivo compensacion", "Motivo"])

    # Si la fecha viene en primera columna sin nombre, la tomamos
    if col_fecha is None and len(df0.columns) > 0:
        col_fecha = df0.columns[0]

    out = pd.DataFrame({
        "Fecha_raw": df0[col_fecha] if col_fecha else None,
        "Dirección de correo electrónico": df0[col_agente] if col_agente else None,
        "Ticket_raw": df0[col_ticket] if col_ticket else None,
        "Correo registrado en Cabify para realizar la carga": df0[col_correo_carga] if col_correo_carga else None,
        "Monto_raw": df0[col_monto] if col_monto else None,
        "Motivo_comp": df0[col_motivo] if col_motivo else None,
    })

    out["Fecha_dt"] = try_parse_datetime(out["Fecha_raw"])
    out["Numero"] = out["Ticket_raw"].apply(normalize_ticket)
    out["Monto_saldo"] = out["Monto_raw"].apply(normalize_amount)
    out["Motivo compensación"] = out["Motivo_comp"].fillna("").astype(str).str.strip()

    out["id_reserva"] = ""  # no viene en esta base
    out["Clasificación_parcial"] = "Aeropuerto - Saldo"

    # Filtrar tickets inválidos
    out = out[out["Numero"].notna()].copy()
    return out[[
        "Fecha_dt", "Dirección de correo electrónico", "Numero",
        "Correo registrado en Cabify para realizar la carga",
        "Monto_saldo", "Motivo compensación",
        "id_reserva", "Clasificación_parcial"
    ]]

def transform_transfer(df: pd.DataFrame) -> pd.DataFrame:
    df0 = df.copy()

    # Columnas típicas de tu formulario
    col_fecha = "Marca temporal" if "Marca temporal" in df0.columns else (df0.columns[0] if len(df0.columns) else None)
    col_agente = "Dirección de correo electrónico" if "Dirección de correo electrónico" in df0.columns else None
    col_motivo = "Motivo" if "Motivo" in df0.columns else None
    col_motivo_aer = "Si es compensación Aeropuerto selecciona el motivo" if "Si es compensación Aeropuerto selecciona el motivo" in df0.columns else None
    col_correo = "Correo" if "Correo" in df0.columns else None
    col_ticket = "Ticket" if "Ticket" in df0.columns else None
    col_monto = "Monto" if "Monto" in df0.columns else None
    col_id_res = "Link payments, link del viaje o numero reserva" if "Link payments, link del viaje o numero reserva" in df0.columns else None

    # Filtrar solo Compensación Aeropuerto
    if col_motivo:
        df0 = df0[df0[col_motivo].astype(str).str.strip().eq("Compensación Aeropuerto")].copy()

    out = pd.DataFrame({
        "Fecha_raw": df0[col_fecha] if col_fecha else None,
        "Dirección de correo electrónico": df0[col_agente] if col_agente else None,
        "Correo registrado en Cabify para realizar la carga": df0[col_correo] if col_correo else None,
        "Ticket_raw": df0[col_ticket] if col_ticket else None,
        "Monto_raw": df0[col_monto] if col_monto else None,
        "Motivo_aer": df0[col_motivo_aer] if col_motivo_aer else None,
        "id_reserva": df0[col_id_res] if col_id_res else None,
    })

    out["Fecha_dt"] = try_parse_datetime(out["Fecha_raw"])
    out["Numero"] = out["Ticket_raw"].apply(normalize_ticket)
    out["Monto_transferencia"] = out["Monto_raw"].apply(normalize_amount)

    # El motivo “equivalente” al de saldos debe ser el de la selección aeropuerto
    out["Motivo compensación"] = out["Motivo_aer"].fillna("").astype(str).str.strip()

    out["Clasificación_parcial"] = "Aeropuerto - Transferencia"

    out["id_reserva"] = out["id_reserva"].fillna("").astype(str).str.strip()

    out = out[out["Numero"].notna()].copy()
    return out[[
        "Fecha_dt", "Dirección de correo electrónico", "Numero",
        "Correo registrado en Cabify para realizar la carga",
        "Monto_transferencia", "Motivo compensación",
        "id_reserva", "Clasificación_parcial"
    ]]

def build_master(df_saldo: pd.DataFrame, df_trans: pd.DataFrame) -> pd.DataFrame:
    # Unir verticalmente, luego agrupar por ticket
    df_all = df_saldo.copy()
    # asegurar columnas faltantes
    if "Monto_saldo" not in df_all.columns:
        df_all["Monto_saldo"] = 0
    if "Monto_transferencia" not in df_all.columns:
        df_all["Monto_transferencia"] = 0

    df_t = df_trans.copy()
    if "Monto_saldo" not in df_t.columns:
        df_t["Monto_saldo"] = 0
    if "Monto_transferencia" not in df_t.columns:
        df_t["Monto_transferencia"] = df_t.get("Monto_transferencia", 0)

    # Alinear columnas
    common_cols = [
        "Fecha_dt", "Dirección de correo electrónico", "Numero",
        "Correo registrado en Cabify para realizar la carga",
        "Monto_saldo", "Monto_transferencia",
        "Motivo compensación",
        "id_reserva", "Clasificación_parcial"
    ]
    df_all = df_all.reindex(columns=common_cols)
    df_t = df_t.reindex(columns=common_cols)

    df_u = pd.concat([df_all, df_t], ignore_index=True)

    # Flags para clasificación final
    df_u["has_saldo"] = df_u["Monto_saldo"].fillna(0).astype(float) > 0
    df_u["has_trans"] = df_u["Monto_transferencia"].fillna(0).astype(float) > 0

    def agg_class(g):
        any_s = bool(g["has_saldo"].any())
        any_t = bool(g["has_trans"].any())
        if any_s and any_t:
            return "Mixto"
        if any_s:
            return "Aeropuerto - Saldo"
        if any_t:
            return "Aeropuerto - Transferencia"
        return ""

    master = (
        df_u.groupby("Numero", as_index=False)
        .agg({
            "Fecha_dt": "min",
            "Dirección de correo electrónico": lambda x: uniq_join(x),
            "Correo registrado en Cabify para realizar la carga": lambda x: uniq_join(x),
            "Monto_saldo": "sum",
            "Monto_transferencia": "sum",
            "Motivo compensación": lambda x: uniq_join(x),
            "id_reserva": lambda x: uniq_join(x),
            "has_saldo": "max",
            "has_trans": "max",
        })
    )

    master["Monto a compensar"] = master["Monto_saldo"].fillna(0).astype(int) + master["Monto_transferencia"].fillna(0).astype(int)
    master["Clasificación"] = master.apply(lambda r: "Mixto" if (r["has_saldo"] and r["has_trans"])
                                           else ("Aeropuerto - Saldo" if r["has_saldo"] else "Aeropuerto - Transferencia"), axis=1)

    # Formato final
    master = master.rename(columns={
        "Fecha_dt": "Fecha",
        "Dirección de correo electrónico": "Dirección de correo electrónico",
        "Numero": "Numero",
        "Correo registrado en Cabify para realizar la carga": "Correo registrado en Cabify para realizar la carga",
    })

    master = master[[
        "Fecha",
        "Dirección de correo electrónico",
        "Numero",
        "Correo registrado en Cabify para realizar la carga",
        "Monto a compensar",
        "Motivo compensación",
        "id_reserva",
        "Clasificación",
        "Monto_saldo",
        "Monto_transferencia",
    ]].sort_values(["Fecha", "Numero"], ascending=[False, True])

    return master

def to_excel_bytes(df: pd.DataFrame, sheet_name="master"):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()

# -----------------------------
# UI
# -----------------------------
st.title("Máster de Compensaciones (Aeropuerto)")

with st.sidebar:
    st.header("Fuentes")
    url_saldos = st.text_input(
        "Google Sheet (Carga de Saldo)",
        value="https://docs.google.com/spreadsheets/d/1Yj8q2dnlKqIZ1-vdr7wZZp_jvXiLoYO6Qwc8NeIeUnE/edit?gid=1139202449#gid=1139202449"
    )
    url_trans = st.text_input(
        "Google Sheet (Transferencias)",
        value="https://docs.google.com/spreadsheets/d/1yHTfTOD-N8VYBSzQRCkaNpMpAQHykBzVB5mYsXS6rHs/edit?resourcekey=&gid=1627777729#gid=1627777729"
    )

    st.caption("Tip: los links deben incluir `gid=` para exportar correctamente la pestaña.")

    st.divider()
    st.header("Alternativa manual (si falla el acceso)")
    up_saldos = st.file_uploader("Subir CSV/XLSX de Carga de Saldo", type=["csv", "xlsx"], key="up_saldos")
    up_trans = st.file_uploader("Subir CSV/XLSX de Transferencias", type=["csv", "xlsx"], key="up_trans")

    run = st.button("Generar Máster", type="primary")

def read_uploaded(file):
    if file is None:
        return None
    name = file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(file)
    if name.endswith(".xlsx"):
        return pd.read_excel(file)
    return None

if run:
    try:
        # Preferir upload si viene, si no descargar por link
        if up_saldos is not None:
            df_s = read_uploaded(up_saldos)
        else:
            df_s = read_gsheet_csv(url_saldos)

        if up_trans is not None:
            df_t = read_uploaded(up_trans)
        else:
            df_t = read_gsheet_csv(url_trans)

        st.success("Fuentes cargadas correctamente.")

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Carga de Saldo (raw)")
            st.dataframe(df_s, use_container_width=True, height=260)
        with c2:
            st.subheader("Transferencias (raw)")
            st.dataframe(df_t, use_container_width=True, height=260)

        df_s2 = transform_saldos(df_s)
        df_t2 = transform_transfer(df_t)

        st.divider()
        c3, c4 = st.columns(2)
        with c3:
            st.subheader("Carga de Saldo (normalizada)")
            st.dataframe(df_s2, use_container_width=True, height=260)
        with c4:
            st.subheader("Transferencias (Aeropuerto, normalizada)")
            st.dataframe(df_t2, use_container_width=True, height=260)

        master = build_master(df_s2, df_t2)

        st.divider()
        st.subheader("Máster por Ticket (1 fila por ticket)")
        st.dataframe(master, use_container_width=True, height=420)

        # Descargas
        excel_bytes = to_excel_bytes(master, sheet_name="master_compensaciones")
        st.download_button(
            "Descargar Máster (Excel)",
            data=excel_bytes,
            file_name="master_compensaciones.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        csv_bytes = master.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Descargar Máster (CSV)",
            data=csv_bytes,
            file_name="master_compensaciones.csv",
            mime="text/csv"
        )

        st.caption("Nota: el caso raro tipo '$28.47' se corrige con heurística (*1000). Si al exportar desde Sheets viene como '28.471', quedará exacto.")

    except Exception as e:
        st.error(f"Error generando el máster: {e}")
        st.stop()
else:
    st.info("Configura los links (o sube archivos) y presiona **Generar Máster**.")
