import sys
import unittest
from hashlib import sha256
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts" / "processing"))
sys.path.insert(0, str(ROOT / "scripts" / "pipeline"))

import adaptador_gcs_silver_v1 as gcs
import puerta_silver_operativa_v1_candidata as gate
import puerta_silver_v1_candidata as core
import fase6_hechos_launcher_documental_v1 as launcher
from fase6_hechos_documental_restringido import (
    classify_official, prepare_document_index
)
from test_fase6_hechos_launcher_documental_v1 import inventory_with_bytes


class TestManifestBytes(unittest.TestCase):
    def setUp(self):
        self.files, _ = inventory_with_bytes()
        total = sum(x["size_bytes"] for x in self.files)

        manifest = {
            "dataset": launcher.DATASET,
            "run_id": launcher.RUN,
            "status": "BRONZE_COMPLETADO",
            "quality_approved": True,
            "silver_consumption_allowed": False,
            "total_bytes": total,
            "files": self.files,
        }

        self.raw = core.canonical_bytes(manifest)
        self.ds = {
            "dataset": launcher.DATASET,
            "run_id": launcher.RUN,
            "bronze_manifest_uri": (
                f"gs://{launcher.CONTROL_BUCKET}/manifests/"
                "bronze/smv/hechos_importancia/manifest.json"
            ),
            "bronze_manifest_generation": "101",
            "bronze_manifest_sha256": sha256(self.raw).hexdigest(),
            "files_count": len(self.files),
            "total_bytes": total,
            "bronze_input_uris": [x["uri"] for x in self.files],
        }

    def test_integracion_manifest_documental(self):
        with patch.object(
            gcs, "read_exact_current_bytes", return_value=self.raw
        ):
            manifest_f5_raw, entries = gcs.read_official_manifest(
                object(), self.ds, core.verify_manifest
            )

        manifest_raw = self.raw
        self.assertEqual(manifest_f5_raw, manifest_raw)

        manifest = gate.core.load_strict(manifest_raw)
        inventory = launcher.validate_official_manifest(
            manifest, entries
        )

        pdf, txt = classify_official(entries)
        index = prepare_document_index(pdf, txt)

        self.assertEqual(inventory["source_object_count"], 1979)
        self.assertEqual(len(index), 990)
        self.assertTrue(launcher.validate_part_capacity(index))

    def test_bytes_distintos_rechazados(self):
        manifest_f5_raw = self.raw
        manifest_raw = self.raw + b" "
        with self.assertRaisesRegex(
            ValueError, "MANIFEST_F5_Y_EXTENSION_DISTINTOS"
        ):
            launcher.require(
                manifest_f5_raw == manifest_raw,
                "MANIFEST_F5_Y_EXTENSION_DISTINTOS",
            )

    def test_json_duplicado_rechazado(self):
        with self.assertRaises(ValueError):
            gate.core.load_strict(b'{"a":1,"a":2}')


if __name__ == "__main__":
    unittest.main()
