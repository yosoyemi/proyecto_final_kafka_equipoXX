"""Panel de ventas legible, con datos en vivo y un tema neutro."""

from __future__ import annotations

import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from obelie_supabase import FiltrosPanel, fechas_consulta

ROOT = Path(__file__).resolve().parents[1]
DATOS = ROOT / "datos"
CSV_VENTAS = DATOS / "ventas.csv"
CSV_CLIENTES = DATOS / "clientes.csv"
CSV_PAGOS = DATOS / "pagos.csv"
CSV_INVENTARIO = DATOS / "inventario.csv"
COLOR = "#475569"

ETIQUETAS = {
    "id_venta": "Venta", "fecha_hora": "Fecha y hora", "producto": "Producto",
    "cantidad": "Unidades", "precio_unitario": "Precio unitario",
    "total": "Total", "sucursal": "Sucursal", "vendedor": "Vendedor",
    "metodo_pago": "Método de pago", "estado": "Estado del pago",
    "monto": "Importe pagado", "stock": "Existencias",
}


def leer_csv(path: Path) -> pd.DataFrame:
    """Tolera archivos ausentes, vacios o una escritura en curso."""
    for intento in range(2):
        try:
            return pd.read_csv(path, encoding="utf-8-sig")
        except (OSError, UnicodeError, pd.errors.EmptyDataError, pd.errors.ParserError):
            if intento == 0:
                time.sleep(0.1)
    return pd.DataFrame()


def preparar(df: pd.DataFrame, numericas: tuple[str, ...] = ()) -> pd.DataFrame:
    df = df.copy()
    if "fecha_hora" in df:
        fechas = pd.to_datetime(df["fecha_hora"], errors="coerce")
        if getattr(fechas.dtype, "tz", None) is not None:
            df["fecha_hora"] = fechas.dt.tz_convert("America/Mexico_City").dt.tz_localize(None)
        else:
            df["fecha_hora"] = fechas
    for columna in numericas:
        if columna in df:
            df[columna] = pd.to_numeric(df[columna], errors="coerce")
    return df


def cargar_desde_supabase(filtros: FiltrosPanel | None = None) -> dict | None:
    """Lee el POS real. None si no hay conexion; los tests pueden parchear esto."""
    from obelie_supabase import cargar_panel_supabase

    return cargar_panel_supabase(filtros)


def cargar_opciones() -> dict[str, list[str]]:
    from catalogo import PRODUCTOS, SUCURSALES
    from obelie_supabase import cargar_opciones_filtros

    try:
        extra = cargar_opciones_filtros()
        if extra.get("sucursales") or extra.get("categorias"):
            return {
                "sucursales": extra.get("sucursales") or [],
                "categorias": extra.get("categorias") or [],
            }
    except Exception:
        pass
    return {
        "sucursales": list(SUCURSALES),
        "categorias": sorted({item["categoria"] for item in PRODUCTOS}),
    }


def _csv_del_panel() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ventas = preparar(leer_csv(CSV_VENTAS), ("total", "cantidad", "precio_unitario"))
    clientes = leer_csv(CSV_CLIENTES)
    pagos = preparar(leer_csv(CSV_PAGOS), ("monto",))
    inventario = preparar(leer_csv(CSV_INVENTARIO), ("stock",))
    return ventas, clientes, pagos, inventario


def filtrar_frame(df: pd.DataFrame, filtros: FiltrosPanel) -> pd.DataFrame:
    if df.empty:
        return df
    salida = df
    if filtros.sucursal and "sucursal" in salida:
        salida = salida[salida["sucursal"] == filtros.sucursal]
    if filtros.producto and "producto" in salida:
        salida = salida[salida["producto"].astype(str).str.contains(filtros.producto, case=False, na=False)]
    if filtros.categoria and "categoria" in salida:
        salida = salida[salida["categoria"].astype(str).str.contains(filtros.categoria, case=False, na=False)]
    if "fecha_hora" in salida and (filtros.desde or filtros.hasta):
        fechas = pd.to_datetime(salida["fecha_hora"], errors="coerce")
        if filtros.desde:
            desde = pd.to_datetime(filtros.desde, utc=True, errors="coerce")
            if pd.notna(desde):
                desde = desde.tz_convert("America/Mexico_City").tz_localize(None)
                salida = salida[fechas >= desde]
                fechas = pd.to_datetime(salida["fecha_hora"], errors="coerce")
        if filtros.hasta:
            hasta = pd.to_datetime(filtros.hasta, utc=True, errors="coerce")
            if pd.notna(hasta):
                hasta = hasta.tz_convert("America/Mexico_City").tz_localize(None)
                salida = salida[fechas <= hasta]
    return salida


def datos_del_panel(
    filtros: FiltrosPanel | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    filtros = filtros or FiltrosPanel()
    extra = {"aviso": "", "fuente": "pos", "n_clientes": 0, "n_tickets": 0, "n_unidades": 0.0}
    cargado = None
    try:
        cargado = cargar_desde_supabase(filtros)
    except Exception as exc:
        extra["aviso"] = f"No hay conexión con Supabase ({exc}). Se muestran los datos de Kafka."
    if cargado:
        ventas = preparar(cargado["ventas"], ("total", "cantidad", "precio_unitario"))
        clientes = cargado.get("clientes")
        if clientes is None or not isinstance(clientes, pd.DataFrame):
            clientes = pd.DataFrame()
        pagos = preparar(cargado["pagos"], ("monto",))
        inventario = preparar(cargado["inventario"], ("stock",))
        extra["n_clientes"] = int(cargado.get("n_clientes") or len(clientes))
    else:
        extra["fuente"] = "kafka"
        if not extra["aviso"]:
            extra["aviso"] = "No hay conexión con Supabase. Se muestran los datos de Kafka."
        ventas, clientes, pagos, inventario = _csv_del_panel()
        extra["n_clientes"] = len(clientes)
    ventas = filtrar_frame(ventas, filtros)
    pagos = filtrar_frame(pagos, filtros)
    inventario = filtrar_frame(inventario, filtros)
    extra["n_tickets"] = int(ventas["id_venta"].nunique()) if "id_venta" in ventas else len(ventas)
    extra["n_unidades"] = float(ventas["cantidad"].sum()) if "cantidad" in ventas else 0.0
    return ventas, clientes, pagos, inventario, extra


def tabla(df: pd.DataFrame) -> None:
    visible = df.rename(columns=ETIQUETAS)
    st.dataframe(
        visible, use_container_width=True, hide_index=True,
        column_config={
            "Fecha y hora": st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm:ss"),
            "Unidades": st.column_config.NumberColumn(format="%g"),
            "Existencias": st.column_config.NumberColumn(format="%g"),
            **{nombre: st.column_config.NumberColumn(format="$ %.2f")
               for nombre in ("Total", "Precio unitario", "Importe pagado")},
        },
    )


def grafica(fig, key: str, *, horizontal: bool = False) -> None:
    fig.update_layout(
        template="plotly_white", paper_bgcolor="#FFFFFF", plot_bgcolor="#FFFFFF",
        font=dict(color="#1F2937", size=13), showlegend=False,
        margin=dict(l=8, r=55, t=12, b=12), height=380,
        colorway=[COLOR],
    )
    fig.update_xaxes(gridcolor="#E5E7EB", zeroline=False, automargin=True)
    fig.update_yaxes(gridcolor="#E5E7EB", zeroline=False, automargin=True)
    if horizontal:
        fig.update_yaxes(autorange="reversed", showgrid=False)
        fig.update_traces(textposition="outside", cliponaxis=False)
    st.plotly_chart(fig, use_container_width=True, theme=None, key=key,
                    config={"displayModeBar": False})


@st.dialog("Filtros", width="large")
def modal_filtros() -> None:
    hoy = date.today()
    guardado: FiltrosPanel = st.session_state.get("filtros_aplicados") or FiltrosPanel()
    opciones = cargar_opciones()
    sucursales = ["Todas"] + (opciones.get("sucursales") or [])
    categorias = ["Todas"] + (opciones.get("categorias") or [])
    sucursal = st.selectbox(
        "Sucursal",
        sucursales,
        index=sucursales.index(guardado.sucursal) if guardado.sucursal in sucursales else 0,
    )
    categoria = st.selectbox(
        "Categoría",
        categorias,
        index=categorias.index(guardado.categoria) if guardado.categoria in categorias else 0,
    )
    producto = st.text_input("Producto", value=guardado.producto, placeholder="Guayaba, Rol de canela…")
    periodo = st.date_input("Periodo", value=(hoy - timedelta(days=6), hoy), format="DD/MM/YYYY")
    aplicar, limpiar = st.columns(2)
    with aplicar:
        aceptar = st.button("Aplicar", type="primary", use_container_width=True)
    with limpiar:
        borrar = st.button("Quitar filtros", use_container_width=True)
    if borrar:
        st.session_state.filtros_aplicados = FiltrosPanel()
        st.rerun()
    if aceptar:
        if isinstance(periodo, (list, tuple)) and len(periodo) == 2:
            fecha_desde, fecha_hasta = periodo
        else:
            fecha_desde = fecha_hasta = periodo
        desde_iso, hasta_iso = fechas_consulta(fecha_desde, fecha_hasta)
        st.session_state.filtros_aplicados = FiltrosPanel(
            sucursal="" if sucursal == "Todas" else sucursal,
            categoria="" if categoria == "Todas" else categoria,
            producto=(producto or "").strip(),
            desde=desde_iso,
            hasta=hasta_iso,
        )
        st.rerun()


@st.fragment(run_every=3)
def panel_en_vivo() -> None:
    filtros = st.session_state.get("filtros_aplicados") or FiltrosPanel()
    ventas, clientes, pagos, inventario, extra = datos_del_panel(filtros)
    if extra.get("aviso"):
        st.warning(extra["aviso"])

    fechas = ventas.get("fecha_hora", pd.Series(dtype="datetime64[ns]")).dropna()
    resumen_filtro = [parte for parte in (filtros.sucursal, filtros.categoria, filtros.producto) if parte]
    detalle_filtro = " · ".join(resumen_filtro) if resumen_filtro else "todas las sucursales"
    origen = "POS Obelie (Supabase)" if extra.get("fuente") == "pos" else "respaldo Kafka (CSV)"
    if fechas.empty:
        st.caption(f"Sin ventas para {detalle_filtro} · {origen} · actualización cada 3 s")
    else:
        ultima = fechas.max()
        ahora = pd.Timestamp.now()
        if getattr(ultima, "tzinfo", None) is not None:
            ahora = pd.Timestamp.now(tz=ultima.tz)
        edad = (ahora - ultima).total_seconds()
        estado = "Recibiendo ventas" if edad < 300 else "Sin ventas nuevas en los últimos minutos"
        st.caption(
            f"{estado} · {origen} · {detalle_filtro} · "
            f"última venta: {ultima:%d/%m/%Y %H:%M:%S}"
        )

    ultimo_stock = pd.DataFrame()
    if {"producto", "sucursal", "stock"}.issubset(inventario.columns):
        base = inventario.dropna(subset=["producto", "sucursal", "stock"])
        if "fecha_hora" in base:
            ultimo_stock = (
                base.sort_values("fecha_hora", kind="stable")
                .drop_duplicates(["producto", "sucursal"], keep="last")
            )
        else:
            ultimo_stock = base
    total = ventas["total"].sum() if "total" in ventas else 0
    stock = ultimo_stock["stock"].sum() if not ultimo_stock.empty else 0
    n_ventas = extra["n_tickets"] if extra.get("n_tickets") is not None else len(ventas)
    n_unidades = extra["n_unidades"]
    if n_unidades is None:
        n_unidades = ventas["cantidad"].sum() if "cantidad" in ventas else 0

    for columna, etiqueta, valor in zip(
        st.columns(4),
        ("Total vendido (MXN)", "Tickets", "Unidades vendidas", "Existencias (piezas)"),
        (f"$ {total:,.2f}", f"{n_ventas:,}", f"{n_unidades:,.0f}", f"{stock:,.0f}"),
    ):
        with columna:
            with st.container(border=True):
                st.metric(etiqueta, valor)
    if extra.get("fuente") == "pos":
        st.caption("Solo transacciones reales del POS en Supabase. Los CSV de Kafka no se mezclan.")
    else:
        st.caption("Respaldo temporal con el streaming de Kafka. El POS no está disponible.")

    st.subheader("Ventas recientes")
    if ventas.empty:
        st.write("Todavía no hay ventas. Al iniciar el sistema, los registros aparecerán aquí.")
    elif "fecha_hora" in ventas:
        recientes = ventas.sort_values("fecha_hora", ascending=False, kind="stable").head(10)
        columnas = ["fecha_hora", "producto", "cantidad", "precio_unitario", "total", "sucursal"]
        tabla(recientes[[c for c in columnas if c in recientes]])
        st.caption("Últimas 10 ventas · importes en pesos mexicanos")
    else:
        st.write("Los registros de ventas no incluyen fecha y hora.")

    st.divider()
    izquierda, derecha = st.columns(2, gap="large")
    with izquierda:
        st.subheader("Ventas a lo largo del tiempo")
        if not ventas.empty and {"fecha_hora", "total"}.issubset(ventas.columns):
            serie = (ventas.dropna(subset=["fecha_hora", "total"])
                     .set_index("fecha_hora").resample("1min")["total"].sum().reset_index())
            if not serie.empty:
                fig = px.line(serie, x="fecha_hora", y="total", color_discrete_sequence=[COLOR],
                              labels={"fecha_hora": "Hora", "total": "Ventas (MXN)"})
                fig.update_traces(line_width=2, hovertemplate="%{x|%d/%m %H:%M}<br>$ %{y:,.2f}<extra></extra>")
                grafica(fig, "ventas_tiempo")
                st.caption("Importe vendido por minuto.")
            else:
                st.write("Esperando ventas con fecha válida.")
        else:
            st.write("Esperando datos de ventas.")

    with derecha:
        st.subheader("Unidades vendidas por producto")
        if not ventas.empty and {"producto", "cantidad"}.issubset(ventas.columns):
            resumen = (
                ventas.groupby("producto", as_index=False)
                .agg(cantidad=("cantidad", "sum"), total=("total", "sum"))
                .sort_values("cantidad", ascending=False)
            )
            fig = px.bar(
                resumen.head(20), x="cantidad", y="producto", orientation="h",
                text="cantidad", color_discrete_sequence=[COLOR],
                hover_data={"total": ":.2f"},
                labels={"producto": "", "cantidad": "Unidades", "total": "Ventas (MXN)"},
            )
            fig.update_traces(texttemplate="%{text:g}")
            grafica(fig, "ventas_producto", horizontal=True)
            st.caption("Suma de cantidades del ticket POS, no del importe.")
        else:
            st.write("Esperando datos de productos.")

    izquierda, derecha = st.columns(2, gap="large")
    with izquierda:
        st.subheader("Métodos de pago")
        if not pagos.empty and "metodo_pago" in pagos:
            metodos = (
                pagos["metodo_pago"].dropna().astype(str).str.strip()
                .value_counts().rename_axis("Método").reset_index(name="Pagos")
            )
            fig = px.bar(metodos, x="Pagos", y="Método", orientation="h", text="Pagos",
                         color_discrete_sequence=[COLOR], labels={"Método": ""})
            grafica(fig, "metodos_pago", horizontal=True)
            st.caption("Número de pagos registrados, incluidos los pendientes.")
        else:
            st.write("Esperando datos de pagos.")

    with derecha:
        st.subheader("Existencias por producto")
        if not ultimo_stock.empty:
            resumen = ultimo_stock.groupby("producto", as_index=False)["stock"].sum().sort_values("stock", ascending=False)
            fig = px.bar(resumen.head(20), x="stock", y="producto", orientation="h", text="stock",
                         color_discrete_sequence=[COLOR], labels={"producto": "", "stock": "Piezas"})
            grafica(fig, "stock_producto", horizontal=True)
            st.caption("Piezas actuales en piso. Usa categoría Pasteles para ocultar velas y bengalas.")
        else:
            st.write("Esperando datos de inventario.")

    with st.expander("Detalle de ventas y pagos"):
        if not ventas.empty and not pagos.empty and "id_venta" in ventas and "id_venta" in pagos:
            campos_pago = [c for c in ("id_venta", "metodo_pago", "estado", "monto") if c in pagos]
            integrado = ventas.merge(pagos[campos_pago], on="id_venta", how="left")
            if "fecha_hora" in integrado:
                integrado = integrado.sort_values("fecha_hora", ascending=False, kind="stable")
            columnas = ["id_venta", "fecha_hora", "producto", "cantidad", "total", "metodo_pago", "estado", "monto"]
            tabla(integrado[[c for c in columnas if c in integrado]].head(30))
            st.caption("Últimas 30 ventas. Un pago vacío puede estar pendiente de recepción.")
        else:
            st.write("El detalle aparecerá al recibir ventas y pagos.")


def main() -> None:
    st.set_page_config(page_title="Obelie | Ventas en tiempo real", layout="wide")
    titulo, accion = st.columns([10, 1], vertical_alignment="bottom")
    with titulo:
        st.title("Obelie")
        st.subheader("Ventas en tiempo real")
        st.caption("Datos reales del POS en Supabase")
    with accion:
        if st.button("Filtros", type="secondary"):
            modal_filtros()
    panel_en_vivo()


if __name__ == "__main__":
    main()
