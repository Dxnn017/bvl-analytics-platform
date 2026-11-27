"""Política candidata Silver-Gold. No autoriza consumo ni publicación."""

DEPENDENCIAS_GOLD = {
    "mercado_valor_diario": {
        "fuentes": [
            "mercado_diario/cotizaciones",
            "macroeconomia/tipo_cambio",
            "macroeconomia/embig",
            "referencias_smv/puente_ruc_nemonico",
        ],
        "estado": "PENDIENTE_PUBLICACION_SILVER",
    },
    "actividad_sab_diaria": {
        "fuentes": ["mercado_diario/montos_sab"],
        "estado": "COBERTURA_PARCIAL",
        "registros_cuarentena": 245,
        "consultas_afectadas": 14,
    },
    "empresa_trimestre": {
        "fuentes": [
            "finanzas_empresariales/principales_cuentas",
        ],
        "estado": "REVISAR_IDENTIDAD_EMPRESARIAL",
    },
    "hechos_empresa": {
        "fuentes": [],
        "estado": "BLOQUEADO_QUALITY",
    },
    "impacto_hechos_valor": {
        "fuentes": [
            "gold/hechos_empresa",
            "gold/mercado_valor_diario",
        ],
        "estado": "BLOQUEADO_QUALITY",
    },
}


def evaluar_producto_gold(producto, habilitaciones=None):
    """Devuelve diagnóstico sin conceder autorización."""
    if producto not in DEPENDENCIAS_GOLD:
        raise ValueError(f"Producto desconocido: {producto}")

    politica = DEPENDENCIAS_GOLD[producto]

    return {
        "producto": producto,
        "estado_quality": politica["estado"],
        "fuentes": politica["fuentes"],
        "habilitacion_oficial_pendiente": True,
        "consumo_productivo_autorizado": False,
        "publicacion_autorizada": False,
    }


def validar_cobertura_sab(
    registros_validos,
    registros_cuarentena,
    consultas_afectadas,
    cobertura_declarada,
):
    """Conserva la cobertura parcial sin inventar actividad."""
    if (
        type(registros_validos) is not int
        or type(registros_cuarentena) is not int
        or type(consultas_afectadas) is not int
        or registros_validos < 0
        or registros_cuarentena < 0
        or consultas_afectadas < 0
    ):
        raise ValueError("Conteos SAB inválidos.")

    if registros_cuarentena != 245 or consultas_afectadas != 14:
        raise ValueError("Evidencia SAB incompatible con Quality.")

    if cobertura_declarada != "PARCIAL":
        raise ValueError("No se permite declarar cobertura completa.")

    return {
        "registros_validos": registros_validos,
        "registros_cuarentena": registros_cuarentena,
        "consultas_afectadas": consultas_afectadas,
        "cobertura": "PARCIAL",
        "completar_faltantes_autorizado": False,
        "publicacion_autorizada": False,
    }


def validar_dependencia_financiera(identidad_acreditada):
    """Bloquea atribuciones empresariales sin evidencia."""
    if identidad_acreditada is not True:
        raise ValueError("Identidad Empresarial no acreditada.")
    return {"validacion_local": True,
            "publicacion_autorizada": False}
