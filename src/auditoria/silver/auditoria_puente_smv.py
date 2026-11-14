
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser(
        description="Auditoria del puente RUC-Nemonico"
    )

    parser.add_argument("--silver-root", required=True)

    args = parser.parse_args()
    root = args.silver_root.rstrip("/")

    spark = (
        SparkSession.builder
        .appName("BVL_Auditoria_Puente_SMV")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        puente = spark.read.parquet(
            f"{root}/referencias_smv/puente_ruc_nemonico"
        )

        cotizaciones = spark.read.parquet(
            f"{root}/mercado_diario/cotizaciones"
        )

        finanzas = spark.read.parquet(
            f"{root}/finanzas_empresariales/principales_cuentas"
        )

        valores = spark.read.parquet(
            f"{root}/referencias_smv/valores_inscritos"
        )

        print("\n" + "=" * 80)
        print("AUDITORIA DEL PUENTE RUC-NEMONICO")
        print("=" * 80)

        print("\n1. INVENTARIO DEL PUENTE")

        print(f"Registros: {puente.count()}")

        print(
            f"RUC distintos: "
            f"{puente.select('ruc').distinct().count()}"
        )

        print(
            f"Nemonicos distintos: "
            f"{puente.select('nemonico_valor').distinct().count()}"
        )

        print("\n2. NEMONICOS ASOCIADOS A VARIOS RUC")

        ambiguos = (
            puente.groupBy("nemonico_valor")
            .agg(
                F.countDistinct("ruc").alias("cantidad_ruc"),
                F.collect_set("ruc").alias("rucs")
            )
            .filter(F.col("cantidad_ruc") > 1)
        )

        print(f"Nemonicos ambiguos: {ambiguos.count()}")

        ambiguos.show(50, truncate=False)

        print("\n3. EMPRESAS CON VARIOS NEMONICOS")

        empresas = (
            puente.groupBy("ruc", "razon_social")
            .agg(
                F.countDistinct("nemonico_valor")
                .alias("cantidad_nemonicos")
            )
            .filter(F.col("cantidad_nemonicos") > 1)
        )

        print(
            f"Grupos empresariales con varios nemonicos: "
            f"{empresas.count()}"
        )

        empresas.orderBy(
            F.desc("cantidad_nemonicos")
        ).show(20, truncate=False)

        print("\n4. COBERTURA SOBRE COTIZACIONES")

        cotizados = cotizaciones.select(
            F.col("valor").alias("nemonico_valor")
        ).distinct()

        puente_nemonicos = puente.select(
            "nemonico_valor"
        ).distinct()

        vinculados = cotizados.join(
            puente_nemonicos,
            "nemonico_valor",
            "left_semi"
        )

        no_vinculados = cotizados.join(
            puente_nemonicos,
            "nemonico_valor",
            "left_anti"
        )

        print(f"Valores cotizados: {cotizados.count()}")
        print(f"Valores vinculados: {vinculados.count()}")
        print(f"Valores sin vinculo: {no_vinculados.count()}")

        no_vinculados.show(30, truncate=False)

        print("\n5. COBERTURA DE EMPRESAS FINANCIERAS")

        finanzas_ruc = (
            finanzas.filter(F.col("ruc_valido") == True)
            .select("ruc")
            .distinct()
        )

        puente_ruc = puente.select("ruc").distinct()

        finanzas_vinculadas = finanzas_ruc.join(
            puente_ruc,
            "ruc",
            "left_semi"
        )

        finanzas_no_vinculadas = finanzas_ruc.join(
            puente_ruc,
            "ruc",
            "left_anti"
        )

        print(f"RUC financieros validos: {finanzas_ruc.count()}")

        print(
            f"RUC financieros vinculados: "
            f"{finanzas_vinculadas.count()}"
        )

        print(
            f"RUC financieros sin vinculo: "
            f"{finanzas_no_vinculadas.count()}"
        )

        print("\n6. COMPARACION CON VALORES INSCRITOS")

        pares_valores = valores.select(
            "ruc",
            "nemonico_valor"
        ).distinct()

        pares_puente = puente.select(
            "ruc",
            "nemonico_valor"
        ).distinct()

        print(f"Pares valores inscritos: {pares_valores.count()}")
        print(f"Pares puente: {pares_puente.count()}")

        print(
            f"Pares exclusivos de valores inscritos: "
            f"{pares_valores.join(pares_puente, ['ruc', 'nemonico_valor'], 'left_anti').count()}"
        )

        print(
            f"Pares exclusivos del puente: "
            f"{pares_puente.join(pares_valores, ['ruc', 'nemonico_valor'], 'left_anti').count()}"
        )

        print("\nAUDITORIA FINALIZADA")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
