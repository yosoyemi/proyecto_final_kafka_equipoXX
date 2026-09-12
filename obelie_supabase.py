"""Cliente de lectura del POS Obelie en Supabase.

Mapea ventas, pagos, clientes e inventario reales al esquema del dashboard.
No escribe en las tablas de produccion.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parent
try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass

SUPABASE_URL = os.environ.get(
    "SUPABASE_URL", "https://rydymvahvuogjuiuyuzq.supabase.co"
).strip()
SUPABASE_PUBLISHABLE_KEY = (
    os.environ.get("SUPABASE_PUBLISHABLE_KEY")
    or os.environ.get("SUPABASE_KEY")
    or os.environ.get("SUPABASE_ANON_KEY")
    or "sb_publishable_onboT65SsEKLULgQk7cmhA_9cmkP7-t"
).strip()

VENTAS_DIAS = 7
PAGOS_DIAS = 7
PAGE = 1000
MAX_FILAS = 4000
TZ_MX = ZoneInfo("America/Mexico_City")

_CACHE: dict[str, tuple[float, Any]] = {}
_CLIENTE = None


@dataclass(frozen=True)
class FiltrosPanel:
    sucursal: str = ""
    producto: str = ""
    categoria: str = ""
    desde: str | None = None
    hasta: str | None = None


def _texto(valor: Any) -> str:
    return " ".join(str(valor or "").split())


def nombre_producto(fila: dict[str, Any]) -> str:
    """Nombre visible del POS: limpia espacios y completa tamaño + sabor si falta."""
    nombre = _texto(
        fila.get("nombre")
        or fila.get("name")
        or fila.get("producto")
        or fila.get("product_name")
    )
    if nombre:
        return nombre
    tamano = _texto(fila.get("tamaño") or fila.get("tamano") or fila.get("size"))
    sabor = _texto(fila.get("sabor") or fila.get("flavor"))
    if tamano and sabor:
        return f"{tamano} de {sabor}"
    return tamano or sabor or "Producto"


def fechas_consulta(desde_local=None, hasta_local=None, dias: int = VENTAS_DIAS) -> tuple[str, str]:
    """Convierte fechas de Mexico a timestamps UTC para filtrar en Supabase."""
    ahora = datetime.now(TZ_MX)
    fin = datetime.combine(hasta_local, datetime.max.time()).replace(tzinfo=TZ_MX) if hasta_local else ahora
    if desde_local:
        inicio = datetime.combine(desde_local, datetime.min.time()).replace(tzinfo=TZ_MX)
    else:
        inicio = fin - timedelta(days=dias)
    return inicio.astimezone(timezone.utc).isoformat(), fin.astimezone(timezone.utc).isoformat()


def _cached(clave: str, ttl: float, cargar):
    ahora = time.monotonic()
    golpe = _CACHE.get(clave)
    if golpe and ahora - golpe[0] < ttl:
        return golpe[1]
    valor = cargar()
    _CACHE[clave] = (ahora, valor)
    return valor


def cliente_supabase():
    """Crea (o reutiliza) el cliente de solo lectura."""
    global _CLIENTE
    if _CLIENTE is not None:
        return _CLIENTE
    from supabase import create_client

    if not SUPABASE_URL or not SUPABASE_PUBLISHABLE_KEY:
        raise RuntimeError("Faltan SUPABASE_URL o SUPABASE_PUBLISHABLE_KEY.")
    _CLIENTE = create_client(SUPABASE_URL, SUPABASE_PUBLISHABLE_KEY)
    return _CLIENTE


def ping_supabase() -> tuple[bool, str]:
    """Comprueba que el proyecto responde y hay ventas reales."""
    try:
        cliente = cliente_supabase()
        respuesta = (
            cliente.table("ventas")
            .select("id,fecha_venta,sucursal,total,ticket_folio")
            .order("fecha_venta", desc=True)
            .limit(1)
            .execute()
        )
    except Exception as exc:
        return False, str(exc)
    filas = respuesta.data or []
    if not filas:
        return True, "Conectado a Supabase, sin ventas todavia."
    venta = filas[0]
    return True, (
        f"Conectado a {SUPABASE_URL} · ultima venta "
        f"{venta.get('ticket_folio') or venta.get('id')} "
        f"{venta.get('sucursal')} ${venta.get('total')}"
    )


def _paginar(armar, *, max_filas: int = MAX_FILAS) -> list[dict]:
    filas: list[dict] = []
    inicio = 0
    while inicio < max_filas:
        fin = min(inicio + PAGE - 1, max_filas - 1)
        respuesta = armar().range(inicio, fin).execute()
        lote = respuesta.data or []
        filas.extend(lote)
        if len(lote) < PAGE:
            break
        inicio += PAGE
    return filas


def _desde(dias: int) -> str:
    punto = datetime.now(timezone.utc) - timedelta(days=dias)
    return punto.isoformat()


def _numero(valor: Any, defecto: float = 0.0) -> float:
    try:
        if valor is None or valor == "":
            return defecto
        return float(valor)
    except (TypeError, ValueError):
        return defecto


def expandir_ventas(filas: list[dict]) -> list[dict]:
    """Una fila por producto de cada ticket POS."""
    lineas: list[dict] = []
    for venta in filas:
        if (venta.get("estado") or "").lower() == "cancelada":
            continue
        identificador = str(venta.get("ticket_folio") or venta.get("id") or "")
        if venta.get("id") is not None:
            id_venta = str(venta["id"])
        else:
            id_venta = identificador
        fecha = venta.get("fecha_venta") or venta.get("fecha_hora")
        sucursal = venta.get("sucursal") or ""
        vendedor = venta.get("usuario") or sucursal
        items = venta.get("items")
        if not isinstance(items, list) or not items:
            total = _numero(venta.get("total"))
            lineas.append(
                {
                    "id_venta": id_venta,
                    "folio": identificador,
                    "fecha_hora": fecha,
                    "producto": "Venta",
                    "cantidad": 1,
                    "precio_unitario": total,
                    "total": total,
                    "sucursal": sucursal,
                    "vendedor": vendedor,
                    "metodo_pago": venta.get("metodo_pago") or "",
                    "estado": venta.get("estado") or "",
                    "categoria": "",
                }
            )
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            cantidad = _numero(item.get("cantidad") or item.get("qty") or item.get("quantity"), 1.0)
            if cantidad <= 0:
                cantidad = 1.0
            precio = _numero(item.get("precio_unitario") or item.get("precio") or item.get("price"))
            total_item = _numero(item.get("total"), round(cantidad * precio, 2))
            lineas.append(
                {
                    "id_venta": id_venta,
                    "folio": identificador,
                    "fecha_hora": fecha,
                    "producto": nombre_producto(item),
                    "cantidad": cantidad,
                    "precio_unitario": precio,
                    "total": total_item,
                    "sucursal": sucursal,
                    "vendedor": vendedor,
                    "metodo_pago": venta.get("metodo_pago") or "",
                    "estado": venta.get("estado") or "",
                    "categoria": _texto(item.get("category_name") or item.get("categoria")),
                }
            )
    return lineas


def mapear_pagos(filas: list[dict]) -> list[dict]:
    mapeo_estado = {
        "activo": "aprobado",
        "completada": "aprobado",
        "completado": "aprobado",
        "cancelado": "cancelado",
        "cancelada": "cancelado",
        "pendiente": "pendiente",
    }
    pagos: list[dict] = []
    for pago in filas:
        crudo = (pago.get("estado") or "activo").strip().lower()
        venta_id = pago.get("venta_id")
        pagos.append(
            {
                "id_pago": f"P-{pago.get('id')}",
                "id_venta": "" if venta_id is None else str(venta_id),
                "fecha_hora": pago.get("fecha_pago") or pago.get("created_at"),
                "metodo_pago": str(pago.get("metodo_pago") or "").strip() or "otro",
                "monto": _numero(pago.get("monto")),
                "estado": mapeo_estado.get(crudo, crudo or "aprobado"),
                "referencia": str(pago.get("detalles_pago") or pago.get("sucursal") or ""),
                "sucursal": _texto(pago.get("sucursal")),
            }
        )
    return pagos


def pagos_desde_ventas(lineas: list[dict]) -> list[dict]:
    """Un pago por ticket, usando el metodo registrado en la venta POS."""
    vistos: set[str] = set()
    pagos: list[dict] = []
    for linea in lineas:
        id_venta = str(linea.get("id_venta") or "")
        if not id_venta or id_venta in vistos:
            continue
        vistos.add(id_venta)
        estado_venta = (linea.get("estado") or "").lower()
        pagos.append(
            {
                "id_pago": f"PV-{id_venta}",
                "id_venta": id_venta,
                "fecha_hora": linea.get("fecha_hora"),
                "metodo_pago": str(linea.get("metodo_pago") or "").strip() or "otro",
                "monto": 0.0,
                "estado": "aprobado" if estado_venta != "cancelada" else "cancelado",
                "referencia": str(linea.get("folio") or ""),
            }
        )
    totales: dict[str, float] = {}
    for linea in lineas:
        id_venta = str(linea.get("id_venta") or "")
        totales[id_venta] = totales.get(id_venta, 0.0) + _numero(linea.get("total"))
    for pago in pagos:
        pago["monto"] = round(totales.get(pago["id_venta"], 0.0), 2)
    return pagos


def agregar_inventario(filas: list[dict]) -> list[dict]:
    """Una fila por producto y sucursal, con el conteo de piezas en piso."""
    grupos: dict[tuple[str, str], dict[str, Any]] = {}
    for fila in filas:
        producto = nombre_producto(fila)
        sucursal = _texto(fila.get("sucursal"))
        clave = (producto, sucursal)
        actual = grupos.get(clave)
        fecha = fila.get("fecha_ingreso")
        piezas = _numero(fila.get("cantidad") or fila.get("stock"), 1.0)
        if piezas <= 0:
            piezas = 1.0
        if actual is None:
            grupos[clave] = {
                "id_evento": f"I-{sucursal}-{producto}"[:80],
                "fecha_hora": fecha,
                "producto": producto,
                "stock": piezas,
                "sucursal": sucursal,
                "categoria": _texto(fila.get("category_name") or fila.get("categoria")) or "Sin categoria",
                "unidad": "pza",
                "costo_unitario": _numero(fila.get("precio")),
            }
            continue
        actual["stock"] += piezas
        if fecha and (not actual["fecha_hora"] or str(fecha) > str(actual["fecha_hora"])):
            actual["fecha_hora"] = fecha
    return list(grupos.values())


def filtrar_lineas(lineas: list[dict], filtros: FiltrosPanel, nombres_categoria: set[str] | None = None) -> list[dict]:
    """Aplica producto/categoria sobre las filas ya bajadas de Supabase."""
    producto = filtros.producto.casefold()
    categoria = filtros.categoria.casefold()
    salida: list[dict] = []
    for linea in lineas:
        nombre = _texto(linea.get("producto"))
        if producto and producto not in nombre.casefold():
            continue
        if nombres_categoria and nombre not in nombres_categoria:
            continue
        if categoria and not nombres_categoria:
            cat = _texto(linea.get("categoria")).casefold()
            if cat and categoria not in cat:
                continue
        salida.append(linea)
    return salida


def _a_frame(filas: list[dict]) -> pd.DataFrame:
    if not filas:
        return pd.DataFrame()
    return pd.DataFrame(filas)


def _con_filtros_tabla(query, filtros: FiltrosPanel, campo_fecha: str):
    if filtros.desde:
        query = query.gte(campo_fecha, filtros.desde)
    if filtros.hasta:
        query = query.lte(campo_fecha, filtros.hasta)
    if filtros.sucursal:
        query = query.eq("sucursal", filtros.sucursal)
    return query


def cargar_opciones_filtros() -> dict[str, list[str]]:
    """Sucursales y categorias reales para los selectores del dashboard."""
    def cargar():
        cliente = cliente_supabase()
        sucursales: list[str] = []
        categorias: list[str] = []
        try:
            ramas = (
                cliente.table("branches")
                .select("name")
                .eq("is_active", True)
                .order("name")
                .execute()
            )
            sucursales = [_texto(r.get("name")) for r in (ramas.data or []) if _texto(r.get("name"))]
        except Exception:
            sucursales = []
        try:
            cats = cliente.table("categories").select("name").order("name").execute()
            categorias = [_texto(r.get("name")) for r in (cats.data or []) if _texto(r.get("name"))]
        except Exception:
            categorias = []
        if not sucursales:
            ventas = (
                cliente.table("ventas")
                .select("sucursal")
                .gte("fecha_venta", _desde(VENTAS_DIAS))
                .limit(1000)
                .execute()
            )
            sucursales = sorted({_texto(r.get("sucursal")) for r in (ventas.data or []) if _texto(r.get("sucursal"))})
        return {"sucursales": sucursales, "categorias": categorias}

    return _cached("opciones_filtros", 60.0, cargar)


def _nombres_de_categoria(cliente, categoria: str) -> set[str]:
    if not categoria:
        return set()

    def cargar():
        nombres: set[str] = set()
        try:
            inv = _paginar(
                lambda: cliente.table("inventario_en_vivo")
                .select("nombre,sabor,category_name")
                .eq("category_name", categoria),
                max_filas=8000,
            )
            nombres.update(nombre_producto(fila) for fila in inv)
        except Exception:
            pass
        try:
            cat = (
                cliente.table("categories")
                .select("id,name")
                .eq("name", categoria)
                .limit(1)
                .execute()
            )
            cat_id = (cat.data or [{}])[0].get("id")
            if cat_id is not None:
                prods = (
                    cliente.table("productos")
                    .select("nombre,sabor")
                    .eq("category_id", cat_id)
                    .execute()
                )
                nombres.update(nombre_producto(fila) for fila in (prods.data or []))
        except Exception:
            pass
        return nombres

    return _cached(f"nombres_cat|{categoria}", 60.0, cargar)


def cargar_panel_supabase(filtros: FiltrosPanel | None = None) -> dict[str, Any]:
    """Descarga el recorte del POS filtrado en Supabase y lo deja listo para el dashboard."""
    filtros = filtros or FiltrosPanel()
    cliente = cliente_supabase()
    desde_ventas, hasta_ventas = filtros.desde, filtros.hasta
    if not desde_ventas or not hasta_ventas:
        auto_desde, auto_hasta = fechas_consulta()
        desde_ventas = desde_ventas or auto_desde
        hasta_ventas = hasta_ventas or auto_hasta
    filtros = FiltrosPanel(
        sucursal=filtros.sucursal,
        producto=filtros.producto,
        categoria=filtros.categoria,
        desde=desde_ventas,
        hasta=hasta_ventas,
    )
    clave = (
        f"panel|{filtros.sucursal}|{filtros.producto}|{filtros.categoria}|"
        f"{filtros.desde}|{filtros.hasta}"
    )

    def cargar():
        def query_ventas():
            return _con_filtros_tabla(
                cliente.table("ventas")
                .select(
                    "id,sucursal,fecha_venta,usuario,metodo_pago,total,estado,ticket_folio,items"
                )
                .neq("estado", "cancelada")
                .order("fecha_venta", desc=True),
                filtros,
                "fecha_venta",
            )

        def query_pagos():
            pagos_filtros = FiltrosPanel(
                sucursal=filtros.sucursal,
                desde=(filtros.desde or "")[:19].replace("T", " "),
                hasta=(filtros.hasta or "")[:19].replace("T", " "),
            )
            return _con_filtros_tabla(
                cliente.table("pagos_pedidos")
                .select(
                    "id,venta_id,fecha_pago,created_at,metodo_pago,monto,estado,detalles_pago,sucursal"
                )
                .order("fecha_pago", desc=True),
                pagos_filtros,
                "fecha_pago",
            )

        def query_inventario():
            consulta = cliente.table("inventario_en_vivo").select(
                "id,sucursal,fecha_ingreso,nombre,sabor,category_name,precio"
            )
            if filtros.sucursal:
                consulta = consulta.eq("sucursal", filtros.sucursal)
            if filtros.categoria:
                consulta = consulta.eq("category_name", filtros.categoria)
            return consulta

        ventas_crudo = _paginar(query_ventas)
        pagos_crudo = _paginar(query_pagos)
        inventario_crudo = _paginar(query_inventario, max_filas=8000)
        clientes_n = (
            cliente.table("clientes").select("id", count="exact", head=True).execute().count or 0
        )
        nombres_cat = _nombres_de_categoria(cliente, filtros.categoria) if filtros.categoria else None
        lineas = filtrar_lineas(expandir_ventas(ventas_crudo), filtros, nombres_cat)
        inventario = filtrar_lineas(agregar_inventario(inventario_crudo), filtros, nombres_cat)
        pagos_pos = mapear_pagos(pagos_crudo)
        if filtros.sucursal:
            pagos_pos = [p for p in pagos_pos if _texto(p.get("sucursal")) == filtros.sucursal or not p.get("sucursal")]
        pagos_tickets = pagos_desde_ventas(lineas)
        ids_con_pago = {p["id_venta"] for p in pagos_pos if p["id_venta"]}
        ids_venta = {fila["id_venta"] for fila in lineas}
        pagos = [p for p in pagos_pos if p["id_venta"] in ids_venta]
        pagos += [p for p in pagos_tickets if p["id_venta"] not in ids_con_pago]
        unidades = sum(_numero(fila.get("cantidad")) for fila in lineas)
        return {
            "ventas": _a_frame(lineas),
            "pagos": _a_frame(pagos),
            "clientes": pd.DataFrame({"id_cliente": range(int(clientes_n))}),
            "inventario": _a_frame(inventario),
            "n_clientes": int(clientes_n),
            "n_tickets": len(ids_venta),
            "n_unidades": unidades,
            "filtros": filtros,
        }

    return _cached(clave, 2.5, cargar)
