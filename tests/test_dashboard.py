"""Pruebas del dashboard con datos controlados y archivos ausentes."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from streamlit.testing.v1 import AppTest
from dashboard import dashboard


class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        base = Path(self.directory.name)
        self.paths = {name: base / (name.lower() + ".csv")
                      for name in ("CSV_VENTAS", "CSV_CLIENTES", "CSV_PAGOS", "CSV_INVENTARIO")}
        self.overrides = patch.multiple(dashboard, **self.paths)
        self.overrides.start()
        self.addCleanup(self.overrides.stop)
        self.supabase = patch.object(dashboard, "cargar_desde_supabase", return_value=None)
        self.supabase.start()
        self.addCleanup(self.supabase.stop)
        self.opciones = patch.object(
            dashboard, "cargar_opciones", return_value={"sucursales": ["Centro", "Jardines"], "categorias": ["Pasteles"]}
        )
        self.opciones.start()
        self.addCleanup(self.opciones.stop)

    def render(self):
        app = AppTest.from_string("from dashboard.dashboard import main\nmain()").run(timeout=20)
        self.assertEqual([e.message for e in app.exception], [])
        return app

    def write(self, name, rows):
        pd.DataFrame(rows).to_csv(self.paths[name], index=False, encoding="utf-8-sig")

    def test_empty_start(self):
        app = self.render()
        self.assertEqual([m.value for m in app.metric], ["$ 0.00", "0", "0", "0"])
        self.assertEqual(len(app.get("plotly_chart")), 0)
        self.assertEqual(app.subheader[0].value, "Ventas en tiempo real")

    def test_data_totals_latest_stock_and_refresh(self):
        ventas = [
            dict(id_venta="V1", fecha_hora="2026-09-11 10:00:00", producto="Pan", cantidad=1, precio_unitario=10, total=10, sucursal="Centro"),
            dict(id_venta="V2", fecha_hora="2026-09-11 10:01:00", producto="Pastel", cantidad=1, precio_unitario=20, total=20, sucursal="Centro"),
        ]
        self.write("CSV_VENTAS", ventas)
        self.write("CSV_CLIENTES", [dict(id_cliente="C1")])
        self.write("CSV_PAGOS", [dict(id_venta="V1", metodo_pago="efectivo", monto=10, estado="aprobado")])
        self.write("CSV_INVENTARIO", [
            dict(producto="Pan", sucursal="Centro", stock=50, fecha_hora="2026-09-11 09:00:00"),
            dict(producto="Pan", sucursal="Centro", stock=8, fecha_hora="2026-09-11 10:00:00"),
            dict(producto="Pan", sucursal="Norte", stock=4, fecha_hora="2026-09-11 10:00:00"),
        ])
        app = self.render()
        self.assertEqual([m.value for m in app.metric], ["$ 30.00", "2", "2", "12"])
        self.assertEqual(app.dataframe[0].value.iloc[0]["Producto"], "Pastel")
        charts = app.get("plotly_chart")
        self.assertEqual(len(charts), 4)
        for chart in charts:
            figure = json.loads(chart.proto.spec)
            self.assertEqual(figure["layout"]["paper_bgcolor"], "#FFFFFF")
            self.assertEqual(figure["layout"]["font"]["color"], "#1F2937")
        self.write("CSV_VENTAS", ventas + [dict(ventas[0], id_venta="V3")])
        app.run()
        self.assertFalse(app.exception)
        self.assertEqual(app.metric[0].value, "$ 40.00")
        self.assertEqual(app.metric[1].value, "3")

    def test_supabase_uses_real_product_names_and_quantities(self):
        panel = {
            "ventas": pd.DataFrame([
                dict(id_venta="114902", fecha_hora="2026-09-11 10:00:00", producto="Mediano de Guayaba",
                     cantidad=2, precio_unitario=480, total=960, sucursal="Jardines"),
            ]),
            "clientes": pd.DataFrame({"id_cliente": [1]}),
            "pagos": pd.DataFrame([dict(id_venta="114902", metodo_pago="Efectivo", monto=960, estado="aprobado")]),
            "inventario": pd.DataFrame([
                dict(producto="Mediano de Guayaba", sucursal="Jardines", stock=10, fecha_hora="2026-09-11 09:00:00"),
            ]),
            "n_tickets": 1,
            "n_unidades": 2,
            "n_clientes": 1,
        }
        with patch.object(dashboard, "cargar_desde_supabase", return_value=panel):
            app = self.render()
        self.assertEqual(app.dataframe[0].value.iloc[0]["Producto"], "Mediano de Guayaba")
        self.assertEqual(app.dataframe[0].value.iloc[0]["Unidades"], 2)
        self.assertEqual([m.value for m in app.metric], ["$ 960.00", "1", "2", "10"])
        self.assertTrue(any("Filtros" in str(item.label) for item in app.button))

    def test_ignora_csv_si_hay_supabase(self):
        self.write("CSV_VENTAS", [
            dict(id_venta="V1", fecha_hora="2026-09-11 10:02:00", producto="Rol de canela",
                 cantidad=4, precio_unitario=42, total=168, sucursal="Sur"),
        ])
        self.write("CSV_PAGOS", [dict(id_venta="V1", metodo_pago="efectivo", monto=168, estado="aprobado")])
        self.write("CSV_INVENTARIO", [dict(producto="Rol de canela", sucursal="Sur", stock=7, fecha_hora="2026-09-11 10:02:00")])
        panel = {
            "ventas": pd.DataFrame([
                dict(id_venta="114902", fecha_hora="2026-09-11 10:01:00", producto="Mediano de Guayaba",
                     cantidad=2, precio_unitario=480, total=960, sucursal="Jardines"),
            ]),
            "pagos": pd.DataFrame([dict(id_venta="114902", metodo_pago="Efectivo", monto=960, estado="aprobado")]),
            "inventario": pd.DataFrame([
                dict(producto="Mediano de Guayaba", sucursal="Jardines", stock=10, fecha_hora="2026-09-11 09:00:00"),
            ]),
            "n_tickets": 1,
            "n_unidades": 2,
            "n_clientes": 0,
        }
        with patch.object(dashboard, "cargar_desde_supabase", return_value=panel):
            app = self.render()
        productos = set(app.dataframe[0].value["Producto"].tolist())
        self.assertEqual(productos, {"Mediano de Guayaba"})
        self.assertNotIn("Rol de canela", productos)
        self.assertEqual(app.metric[0].value, "$ 960.00")
        self.assertEqual(app.metric[1].value, "1")
        self.assertEqual(app.metric[2].value, "2")
        self.assertEqual(app.metric[3].value, "10")

    def test_incomplete_or_invalid_rows_do_not_crash(self):
        self.write("CSV_VENTAS", [dict(producto="Pan", total="dato incompleto", fecha_hora="sin fecha")])
        self.write("CSV_INVENTARIO", [dict(producto="Pan", stock=4)])
        app = self.render()
        self.assertEqual(app.metric[0].value, "$ 0.00")
        self.assertEqual(app.metric[3].value, "0")


if __name__ == "__main__":
    unittest.main()
