"""Genera dos snapshots Gold sintéticos deterministas en CSV."""
import argparse
import csv
import math
from datetime import date, timedelta
from pathlib import Path

TABLES = ("mercado_valor_diario", "actividad_sab_diaria", "empresa_trimestre")
RATIOS = ("roa_aproximado_pct", "roe_aproximado_pct", "endeudamiento_pct",
          "margen_neto_pct", "autonomia_financiera_pct")


def generate(root, lot):
    root.mkdir(parents=True, exist_ok=True)
    market, sab, firms = [], [], []
    for i in range(9):
        symbol, ruc = f"SIM{i}", str(20000000000 + i)
        firms.append(dict(id_registro=f"REG{i}", rpj=f"RPJ{i}", ruc=ruc,
                          nombre_empresa=symbol, periodo="2023-T4", anio=2023, trimestre=4,
                          fecha_fin_trimestre="2023-12-31", fecha_ingesta="2024-01-08",
                          moneda="PEN", apto_kpi_general=True, activo_total=1000,
                          pasivo_total=400, patrimonio_total=600, total_ingreso=300,
                          utilidad_neta=30, **dict(zip(RATIOS, [3, 5, 40, 10, 60]))))
        price, last = 100, None
        for day in range(180 + 4 * (lot == 2)):
            d = date(2024, 1, 1) + timedelta(days=day)
            if d.weekday() >= 5 or (day + i) % 13 == 0:
                continue
            change = (0.45, 0.45, 0.45, 1.6, 1.6, 4.2)[(i + day // 31) % 6] * math.sin(day + i)
            close = price * (1 + change / 100)
            market.append(dict(fecha=d, nemonico=symbol, nemonico_original=symbol,
                               ruc=ruc, codigo_isin=f"SIMULADO{i}", precio_apertura=price,
                               precio_cierre=close, precio_maximo=max(price, close)*1.001,
                               precio_minimo=min(price, close)*0.999, precio_anterior=price,
                               precio_promedio=(close+price)/2, moneda_referencia="PEN",
                               monto_negociado=10000+day*5+i+(50000 if lot==2 and day==150 and i==0 else 0),
                               retorno_diario_pct=change, rango_intradia_pct=100*(max(price,close)*1.001-min(price,close)*0.999)/price,
                               dias_desde_cierre_anterior=(d-last).days if last else None,
                               tipo_cambio_venta=3.75+0.02*math.sin(day), embig_peru=150+day%12))
            price, last = close, d
    for d in sorted({row["fecha"] for row in market}):
        for j, share in enumerate((60, 40)):
            sab.append(dict(fecha=d, codigo_sab=f"S{j}", sociedad_agente_bolsa=f"SAB {j}",
                            monto_compras=share*5, monto_ventas=share*5, monto_total=share*10,
                            saldo_neto=0, participacion_compras_pct=share,
                            participacion_ventas_pct=share, participacion_total_pct=share,
                            ranking_diario=j+1, hhi_diario=5200, estado_cobertura="COMPLETA",
                            cobertura_completa=True, trazabilidad_verificada=True))
    for table, rows in zip(TABLES, (market, sab, firms)):
        with (root / f"{table}.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--lote", type=int, choices=(1, 2), default=1)
    p.add_argument("--dir", type=Path, default=Path("data/sample"))
    args = p.parse_args()
    generate(args.dir, args.lote)
