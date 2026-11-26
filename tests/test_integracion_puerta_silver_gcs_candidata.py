"""Pruebas unitarias de integracion, solo con mocks, sin acceso Cloud."""
import hashlib
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts' / 'pipeline'))
import integracion_puerta_silver_gcs_candidata as bridge


class TestIntegracionCandidata(unittest.TestCase):
    def setUp(self):
        self.ds = {
            'dataset': 'smv/cotizaciones', 'run_id': 'run123',
            'bronze_manifest_uri': 'gs://bvl26-control-7f3a9c/manifests/bronze/smv/cotizaciones/run_id=run123/manifest.json',
            'bronze_manifest_generation': '123', 'bronze_manifest_sha256': 'a' * 64,
        }
        self.contract = {
            'run_id': 'run123', 'phase_5_closed': False,
            'datasets': [self.ds], 'authorization_protocol': {'issued_authorizations': []},
        }
        self.client = Mock()

    def test_01_bronze_manifest_conectado_solo_lectura(self):
        entries = [{'size_bytes': 9}, {'size_bytes': 11}]
        with patch.object(bridge.puerta, 'validate_contract', return_value=self.contract), \
             patch.object(bridge.gcs, 'read_official_manifest', return_value=(b'{}', entries)) as read:
            result = bridge.inspect_bronze_manifest_readonly(
                self.client, b'contrato', 'a'*64, 'smv/cotizaciones', 'run123')
        self.assertEqual(result['files_count'], 2)
        self.assertEqual(result['total_bytes'], 20)
        self.assertFalse(result['silver_autorizado'])
        self.assertFalse(result['inputs_reales_verificados'])
        self.assertEqual(result['manifest_sha256'], hashlib.sha256(b'{}').hexdigest())
        read.assert_called_once()

    def test_02_rechaza_dataset_ajeno_antes_gcs(self):
        with patch.object(bridge.puerta, 'validate_contract', return_value=self.contract), \
             patch.object(bridge.gcs, 'read_official_manifest') as read:
            with self.assertRaises(bridge.IntegrationError):
                bridge.inspect_bronze_manifest_readonly(self.client,b'x','a'*64,'smv/desconocido','run123')
            read.assert_not_called()

    def test_03_rechaza_run_ajeno_antes_gcs(self):
        with patch.object(bridge.puerta, 'validate_contract', return_value=self.contract), \
             patch.object(bridge.gcs, 'read_official_manifest') as read:
            with self.assertRaises(bridge.IntegrationError):
                bridge.inspect_bronze_manifest_readonly(self.client,b'x','a'*64,'smv/cotizaciones','otro')
            read.assert_not_called()

    def test_04_fase5_bloquea_auth_antes_de_gcs(self):
        with patch.object(bridge.puerta, 'validate_contract', return_value=self.contract), \
             patch.object(bridge.gcs, 'read_signed_authorization_versioned') as read:
            with self.assertRaisesRegex(bridge.IntegrationError, 'Fase 5'):
                bridge.inspect_signed_authorization_readonly(
                    self.client,b'x','a'*64,'smv/cotizaciones','run123',
                    auth_uri='gs://bvl26-control-7f3a9c/authorizations/silver/smv/cotizaciones/run_id=run123/authorization.json',
                    auth_generation='1',auth_sha256='a'*64,
                    signature_uri='gs://bvl26-control-7f3a9c/authorizations/silver/smv/cotizaciones/run_id=run123/authorization.sig',
                    signature_generation='2',signature_sha256='b'*64,
                    trusted_public_key_pem=b'test',attempt_id='attempt-001')
            read.assert_not_called()

    def test_05_mapeo_pendiente_bloquea_auth_antes_de_gcs(self):
        self.contract['phase_5_closed'] = True
        with patch.object(bridge.puerta, 'validate_contract', return_value=self.contract), \
             patch.object(bridge.gcs, 'read_signed_authorization_versioned') as read:
            with self.assertRaisesRegex(bridge.IntegrationError, 'Mapeo'):
                bridge.inspect_signed_authorization_readonly(
                    self.client,b'x','a'*64,'smv/cotizaciones','run123',
                    auth_uri='x',auth_generation='1',auth_sha256='a'*64,
                    signature_uri='y',signature_generation='2',signature_sha256='b'*64,
                    trusted_public_key_pem=b'test',attempt_id='attempt-001')
            read.assert_not_called()

    def test_06_no_crea_claim_automaticamente(self):
        self.assertFalse(hasattr(bridge, 'create_exclusive'))
        self.assertFalse(hasattr(bridge, 'claim_once'))


if __name__ == '__main__':
    unittest.main()
