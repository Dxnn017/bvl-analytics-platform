"""Pruebas HTTP reales exclusivamente sobre servidor 127.0.0.1 temporal."""
import importlib.util
import pathlib
import sys
import unittest
from urllib.parse import parse_qs, urlsplit

MODULE = pathlib.Path(__file__).resolve().parents[1] / 'scripts/ingestion/verificar_multipart_e786_offline.py'
spec = importlib.util.spec_from_file_location('bvl_e786_http', MODULE)
m = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = m
spec.loader.exec_module(m)


class HttpMultipartLocalTests(unittest.TestCase):
    def setUp(self):
        self.server, self.thread = m.start_server()
        self.base = f'http://127.0.0.1:{self.server.server_port}'

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)

    def build(self, file_bytes=b'datos-prueba'):
        return m.build_multipart_request(base_url=self.base, object_name=m.DESTINATION,
                                         file_bytes=file_bytes)

    def test_request_method_path_and_precondition(self):
        req = self.build()
        u = urlsplit(req.full_url)
        self.assertEqual(req.get_method(), 'POST')
        self.assertEqual(u.path, f'/upload/storage/v1/b/{m.BUCKET}/o')
        self.assertEqual(parse_qs(u.query), {'uploadType': ['multipart'], 'ifGenerationMatch': ['0']})

    def test_request_body_multipart_related_metadata_and_contents(self):
        req = self.build(b'contenido-controlado')
        self.assertIn('multipart/related', req.get_header('Content-type'))
        self.assertIn(b'"name":"landing/e786/archivo_sintetico_9mib.bin"', req.data)
        self.assertIn(b'contenido-controlado', req.data)
        self.assertNotIn(b'uploadType=resumable', req.data)

    def test_more_than_8_mib_through_http_loopback(self):
        raw = b'Z' * m.SYNTHETIC_SIZE
        receipt = m.send_to_loopback_only(self.build(raw))
        self.assertEqual(receipt, 'HTTP_MULTIPART_201_LOCAL_CONFIRMADO_SIN_SESSION_URI')
        capture = self.server.received[0]
        self.assertGreater(len(raw), 8 * 1024 * 1024)
        self.assertGreater(capture['body_size'], 8 * 1024 * 1024)
        self.assertEqual(capture['query']['uploadType'], ['multipart'])
        self.assertEqual(capture['query']['ifGenerationMatch'], ['0'])
        self.assertEqual(capture['body'].count(raw), 1)

    def test_response_with_location_rejected(self):
        self.server.synthetic_location = True
        with self.assertRaisesRegex(m.Blocked, 'SESSION_URI'):
            m.send_to_loopback_only(self.build())

    def test_conflict_412_no_retry(self):
        self.server.synthetic_status = 412
        with self.assertRaisesRegex(m.Blocked, 'CONFLICTO_412_NO_REINTENTAR'):
            m.send_to_loopback_only(self.build())
        self.assertEqual(len(self.server.received), 1)

    def test_http_500_fail_closed_no_retry(self):
        self.server.synthetic_status = 500
        with self.assertRaisesRegex(m.Blocked, 'ERROR_HTTP_FAIL_CLOSED'):
            m.send_to_loopback_only(self.build())
        self.assertEqual(len(self.server.received), 1)

    def test_no_resumable_allowed(self):
        with self.assertRaisesRegex(m.Blocked, 'PROTOCOLO'):
            m.build_multipart_request(base_url=self.base, object_name=m.DESTINATION,
                                      file_bytes=b'x', upload_type='resumable')

    def test_no_overwrite_permitted(self):
        with self.assertRaisesRegex(m.Blocked, 'PROTOCOLO'):
            m.build_multipart_request(base_url=self.base, object_name=m.DESTINATION,
                                      file_bytes=b'x', if_generation_match=123)

    def test_no_int_or_string_precondition(self):
        with self.assertRaisesRegex(m.Blocked, 'PROTOCOLO'):
            m.build_multipart_request(base_url=self.base, object_name=m.DESTINATION,
                                      file_bytes=b'x', if_generation_match='0')

    def test_no_external_hosts_or_real_cloud(self):
        for url in ['https://storage.googleapis.com', 'https://127.0.0.1:99',
                    'http://storage.googleapis.com:80', 'http://example.com:80']:
            with self.subTest(url=url), self.assertRaisesRegex(m.Blocked, 'SOLO_SERVIDOR_HTTP_LOCAL'):
                m.build_multipart_request(base_url=url, object_name=m.DESTINATION,
                                          file_bytes=b'x')

    def test_only_landing_scope(self):
        for obj in ['otro/test', '../landing/a', 'landing/../other', 'landing/', 'landing/\\foo']:
            with self.subTest(obj=obj), self.assertRaisesRegex(m.Blocked, 'DESTINO_FUERA_DE_LANDING'):
                m.build_multipart_request(base_url=self.base, object_name=obj,
                                          file_bytes=b'x')

    def test_empty_payload_rejected(self):
        with self.assertRaisesRegex(m.Blocked, 'CONTENIDO_INVALIDO'):
            self.build(b'')

    def test_no_live_client_dependencies(self):
        src = MODULE.read_text(encoding='utf-8')
        self.assertNotIn('google.cloud', src)
        self.assertNotIn('google.auth', src)
        self.assertNotIn('subprocess', src)


if __name__ == '__main__':
    unittest.main()
