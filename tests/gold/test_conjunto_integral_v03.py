"""Prueba integral local del marcador conjunto v0.3."""

import hashlib
import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

from autorizacion_gold_quality_v03 import KEY_VERSION
from verificar_conjunto_silver_v03 import verificar_conjunto_silver


PRODUCTOS = (
    "referencias_smv/valores_inscritos",
    "referencias_smv/puente_ruc_nemonico",
)

BUCKET = "gs://bvl26-control-7f3a9c/"


class TestConjuntoIntegralV03(unittest.TestCase):

    def test_conjunto_firmado_completo(self):
        # Clave efímera: fixture reproducible, sin PEM privado.
        privada = ec.generate_private_key(ec.SECP256R1())
        publica = privada.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )

        huella = hashlib.sha256(
            privada.public_key().public_bytes(
                serialization.Encoding.DER,
                serialization.PublicFormat.SubjectPublicKeyInfo,
            )
        ).hexdigest()

        objetos = {}

        class BlobFalso:
            def __init__(self, uri, generation=None):
                self.uri = uri
                self.solicitada = generation
                self.generation = 123

            def reload(self):
                if self.uri not in objetos:
                    raise FileNotFoundError(self.uri)

            def download_as_bytes(self, if_generation_match=None):
                if self.uri not in objetos:
                    raise FileNotFoundError(self.uri)
                if self.solicitada != 123 or if_generation_match != 123:
                    raise ValueError("Generación incorrecta")
                return objetos[self.uri]

        class BucketFalso:
            def __init__(self, nombre):
                self.nombre = nombre

            def blob(self, nombre, generation=None):
                return BlobFalso(
                    f"gs://{self.nombre}/{nombre}", generation
                )

        class ClienteFalso:
            def bucket(self, nombre):
                return BucketFalso(nombre)

        cliente = ClienteFalso()

        def guardar(uri, contenido):
            ref = {
                "uri": uri,
                "generation": "123",
                "sha256": hashlib.sha256(contenido).hexdigest(),
                "size_bytes": len(contenido),
            }
            objetos[uri] = contenido
            return ref

        def firmar(datos):
            return privada.sign(
                datos, ec.ECDSA(hashes.SHA256())
            )

        versiones = []

        for producto in PRODUCTOS:
            version_id = "fixture-v1"
            prefijo = (
                "gs://bvl26-silver-7f3a9c/versions/"
                f"{producto}/version={version_id}/"
            )

            import io
            import pyarrow as pa
            import pyarrow.parquet as pq

            buffer = io.BytesIO()
            pq.write_table(
                pa.table({"id": pa.array([1], type=pa.int64())}),
                buffer,
            )
            ref_parquet = guardar(
                prefijo + "anio=2026/mes=11/part.parquet",
                buffer.getvalue(),
            )
            ref_parquet["rows"] = 1

            manifest = {
                "schema_version": "BVL-SILVER-INTERNAL-MANIFEST-v1",
                "status": "SILVER_INTERNAL_PUBLISHED",
                "project_id": "bvl-analytics-2026",
                "product": producto,
                "version_id": version_id,
                "run_id": "run-fixture",
                "attempt_id": "attempt-fixture",
                "batch_id": "batch-fixture",
                "processor_commit": "a" * 40,
                "publication_scope": "INTERNAL_SILVER_ONLY",
                "gold_consumption_allowed": False,
                "version_prefix": prefijo,
                "files": [ref_parquet],
                "files_count": 1,
                "bytes_total": ref_parquet["size_bytes"],
                "records_count": 1,
                "schema": "id: int64",
                "partition_columns": ["anio", "mes"],
            }

            manifest_bytes = json.dumps(
                manifest, sort_keys=True
            ).encode()

            base = (
                BUCKET + f"manifests/silver/{producto}/"
                f"version_id={version_id}/"
            )

            ref_manifest = guardar(
                base + "manifest.json", manifest_bytes
            )
            ref_firma = guardar(
                base + "manifest.sig", firmar(manifest_bytes)
            )

            current = {
                "schema_version": "BVL-SILVER-INTERNAL-REGISTRY-v1",
                "status": "SILVER_CURRENT_INTERNAL",
                "project_id": "bvl-analytics-2026",
                "product": producto,
                "version_id": version_id,
                "run_id": "run-fixture",
                "attempt_id": "attempt-fixture",
                "batch_id": "batch-fixture",
                "publication_scope": "INTERNAL_SILVER_ONLY",
                "gold_consumption_allowed": False,
                "version_prefix": prefijo,
                "manifest_uri": ref_manifest["uri"],
                "manifest_generation": ref_manifest["generation"],
                "manifest_sha256": ref_manifest["sha256"],
                "manifest_signature": ref_firma,
            }

            ref_current = guardar(
                BUCKET + f"version_registry/silver/{producto}/current.json",
                json.dumps(current, sort_keys=True).encode(),
            )

            versiones.append({
                "product": producto,
                "version_id": version_id,
                "run_id": "run-fixture",
                "attempt_id": "attempt-fixture",
                "batch_id": "batch-fixture",
                "current": ref_current,
                "manifest": ref_manifest,
                "manifest_signature": ref_firma,
            })

        marcador = {
            "schema_version": "BVL-SILVER-JOINT-PUBLICATION-CANDIDATE-v0.3",
            "status": "SILVER_JOINT_COMMITTED",
            "joint_publication_id": "fixture-joint",
            "project_id": "bvl-analytics-2026",
            "run_id": "run-fixture",
            "attempt_id": "attempt-fixture",
            "batch_id": "batch-fixture",
            "processor_commit": "a" * 40,
            "products": versiones,
            "confirmed_at_utc": datetime.now(
                timezone.utc
            ).isoformat(),
            "key_version": KEY_VERSION,
            "quality_approved": True,
            "gold_consumption_allowed": False,
        }

        contenido = json.dumps(
            marcador, sort_keys=True
        ).encode()

        ref_payload = guardar(
            BUCKET + "joint/fixture.json", contenido
        )
        ref_signature = guardar(
            BUCKET + "joint/fixture.sig", firmar(contenido)
        )

        principal = versiones[0]
        autorizacion = {
            **principal,
            "processor_commit": "a" * 40,
            "joint_marker": {
                "payload": ref_payload,
                "signature": ref_signature,
            },
        }

        with (
            patch(
                "marcador_conjunto_quality_v03.HUELLA_OFICIAL",
                huella,
            ),
        ):
            resultado = verificar_conjunto_silver(
                cliente, autorizacion, publica
            )

            # Prueba positiva de integración del flujo principal.
            from datetime import timedelta
            from autorizacion_gold_quality_v03 import CAMPOS
            from flujo_silver_gold_v03 import preparar_silver_v03

            ahora = datetime.now(timezone.utc)
            auth = {
                "schema_version":
                    "BVL-GOLD-CONSUMPTION-AUTHORIZATION-CANDIDATE-v0.3",
                "status": "GOLD_CONSUMPTION_AUTHORIZED",
                "authorization_id": "fixture-auth-joint",
                "project_id": "bvl-analytics-2026",
                "region": "southamerica-west1",
                "product": principal["product"],
                "version_id": principal["version_id"],
                "run_id": "run-fixture",
                "attempt_id": "attempt-fixture",
                "batch_id": "batch-fixture",
                "processor_commit": "a" * 40,
                "consumer_service_account":
                    "sa-bvl-gold@bvl-analytics-2026.iam.gserviceaccount.com",
                "current": principal["current"],
                "manifest": principal["manifest"],
                "manifest_signature": principal["manifest_signature"],
                "joint_marker": autorizacion["joint_marker"],
                "key_version": KEY_VERSION,
                "quality_approved": True,
                "gold_consumption_allowed": True,
                "not_before_utc": (ahora - timedelta(minutes=1)).isoformat(),
                "expires_at_utc": (ahora + timedelta(minutes=10)).isoformat(),
                "restrictions": {
                    "allowed_gold_products": ["mercado_valor_diario"],
                    "publication_scope": "INTERNAL_SILVER_ONLY",
                },
            }

            self.assertEqual(set(auth), CAMPOS)

            auth_bytes = json.dumps(auth, sort_keys=True).encode()
            auth_firma = firmar(auth_bytes)

            with (
                patch(
                    "autorizacion_gold_quality_v03.HUELLA_OFICIAL",
                    huella,
                ),
            ):
                integrado = preparar_silver_v03(
                    cliente,
                    auth_bytes,
                    auth_firma,
                    publica,
                    "mercado_valor_diario",
                )

            # Flujo completo: autorización, conjunto, GCS y Parquet Hive.
            from flujo_silver_gold_v03 import preparar_silver_v03_con_parquet

            manifest_principal = json.loads(
                objetos[principal["manifest"]["uri"]]
            )
            uri_parquet = manifest_principal["files"][0]["uri"]

            with (
                patch(
                    "autorizacion_gold_quality_v03.HUELLA_OFICIAL",
                    huella,
                ),
                patch(
                    "marcador_conjunto_quality_v03.HUELLA_OFICIAL",
                    huella,
                ),
            ):
                completo = preparar_silver_v03_con_parquet(
                    cliente,
                    auth_bytes,
                    auth_firma,
                    publica,
                    "mercado_valor_diario",
                    {"id": "int64", "anio": "int32", "mes": "int32"},
                    ["id"],
                    {uri_parquet: {"anio": 2026, "mes": 11}},
                )

            self.assertEqual(completo["filas_validadas"], 1)
            self.assertEqual(completo["archivos_parquet_verificados"], 1)
            self.assertFalse(completo["consumo_productivo_autorizado"])

            self.assertTrue(integrado["firma_manifest_verificada"])
            self.assertTrue(integrado["autorizacion_firmada_verificada"])
            self.assertFalse(integrado["consumo_productivo_autorizado"])

        self.assertTrue(resultado["marcador_firmado_verificado"])
        self.assertEqual(resultado["productos_conjuntos_verificados"], 2)
        self.assertFalse(resultado["consumo_productivo_autorizado"])


if __name__ == "__main__":
    unittest.main()
