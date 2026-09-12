"""Pruebas del mapeo POS Supabase -> esquema del dashboard."""

import unittest

from obelie_supabase import (
    FiltrosPanel,
    agregar_inventario,
    expandir_ventas,
    filtrar_lineas,
    mapear_pagos,
    nombre_producto,
    pagos_desde_ventas,
)


class MapeoSupabaseTests(unittest.TestCase):
    def test_expande_items_de_un_ticket(self):
        filas = [
            {
                "id": 114899,
                "sucursal": "Canteras",
                "fecha_venta": "2026-09-12T00:06:27+00:00",
                "usuario": "Canteras",
                "metodo_pago": "Efectivo",
                "total": "525.00",
                "estado": "completada",
                "ticket_folio": "0A1I61J1",
                "items": [
                    {"nombre": "Mediano de Dulce de Leche", "cantidad": 1, "precio_unitario": 480, "total": 480},
                    {"nombre": "Velita", "cantidad": 1, "precio_unitario": 45, "total": 45},
                ],
            }
        ]
        lineas = expandir_ventas(filas)
        self.assertEqual(len(lineas), 2)
        self.assertEqual(lineas[0]["id_venta"], "114899")
        self.assertEqual(lineas[0]["producto"], "Mediano de Dulce de Leche")
        self.assertEqual(lineas[1]["total"], 45)
        self.assertEqual(lineas[0]["metodo_pago"], "Efectivo")

    def test_omite_ventas_canceladas_y_expande_sin_items(self):
        filas = [
            {"id": 1, "estado": "cancelada", "items": [{"nombre": "X", "cantidad": 1, "total": 10}], "total": 10},
            {"id": 2, "estado": "completada", "items": None, "total": 80, "sucursal": "Jardines", "usuario": "Jardines"},
        ]
        lineas = expandir_ventas(filas)
        self.assertEqual(len(lineas), 1)
        self.assertEqual(lineas[0]["producto"], "Venta")
        self.assertEqual(lineas[0]["total"], 80)

    def test_mapea_pagos_y_estados(self):
        pagos = mapear_pagos(
            [
                {"id": 10, "venta_id": 114899, "fecha_pago": "2026-09-11T17:08:25", "metodo_pago": "Santander", "monto": 500, "estado": "activo"},
                {"id": 11, "venta_id": None, "metodo_pago": "Efectivo", "monto": 20, "estado": "cancelado"},
            ]
        )
        self.assertEqual(pagos[0]["id_venta"], "114899")
        self.assertEqual(pagos[0]["estado"], "aprobado")
        self.assertEqual(pagos[1]["id_venta"], "")
        self.assertEqual(pagos[1]["estado"], "cancelado")

    def test_pago_por_ticket_suma_lineas(self):
        lineas = expandir_ventas(
            [
                {
                    "id": 5,
                    "estado": "completada",
                    "metodo_pago": "BBVA",
                    "ticket_folio": "ABC",
                    "items": [
                        {"nombre": "A", "cantidad": 1, "total": 100},
                        {"nombre": "B", "cantidad": 2, "total": 40},
                    ],
                }
            ]
        )
        pagos = pagos_desde_ventas(lineas)
        self.assertEqual(len(pagos), 1)
        self.assertEqual(pagos[0]["monto"], 140)
        self.assertEqual(pagos[0]["metodo_pago"], "BBVA")
        self.assertEqual(pagos[0]["referencia"], "ABC")

    def test_nombre_producto_limpia_y_completa_tamano_sabor(self):
        self.assertEqual(nombre_producto({"nombre": "  Mediano de Guayaba  "}), "Mediano de Guayaba")
        self.assertEqual(
            nombre_producto({"nombre": "", "tamaño": "Chico", "sabor": "Dulce de Leche"}),
            "Chico de Dulce de Leche",
        )

    def test_filtra_por_producto(self):
        lineas = expandir_ventas(
            [
                {
                    "id": 1,
                    "estado": "completada",
                    "items": [
                        {"nombre": "Mediano de Guayaba", "cantidad": 2, "total": 960},
                        {"nombre": "Bengala", "cantidad": 1, "total": 45},
                    ],
                }
            ]
        )
        filtradas = filtrar_lineas(lineas, FiltrosPanel(producto="guayaba"))
        self.assertEqual(len(filtradas), 1)
        self.assertEqual(filtradas[0]["cantidad"], 2)

    def test_inventario_cuenta_piezas_por_producto_y_sucursal(self):
        filas = [
            {"nombre": "Obelito de Guayaba", "sucursal": "Pocitos", "category_name": "Pasteles", "precio": 130, "fecha_ingreso": "2026-09-09T19:50:23+00:00"},
            {"nombre": "Obelito de Guayaba", "sucursal": "Pocitos", "category_name": "Pasteles", "precio": 130, "fecha_ingreso": "2026-09-10T10:00:00+00:00"},
            {"nombre": "Chamuco", "sucursal": "Central", "category_name": "Chamucos", "precio": 60, "fecha_ingreso": "2026-09-01T00:00:00+00:00"},
        ]
        resumen = agregar_inventario(filas)
        por_clave = {(r["producto"], r["sucursal"]): r for r in resumen}
        self.assertEqual(por_clave[("Obelito de Guayaba", "Pocitos")]["stock"], 2)
        self.assertEqual(por_clave[("Chamuco", "Central")]["stock"], 1)
        self.assertEqual(por_clave[("Obelito de Guayaba", "Pocitos")]["fecha_hora"], "2026-09-10T10:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
