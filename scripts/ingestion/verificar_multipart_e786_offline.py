#!/usr/bin/env python3
"""E.7.8.6: prueba HTTP local, NO es ejecutor Cloud ni permite transferencias.

Construye una peticion JSON API multipart/related y la envia SOLO a loopback
contra un servidor HTTP sintetico. No se importan credenciales, IAM ni GCS.
"""
from __future__ import annotations

import hashlib
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlsplit
from urllib.request import Request, urlopen

BUCKET = 'bvl26-bronze-7f3a9c'
DESTINATION = 'landing/e786/archivo_sintetico_9mib.bin'
BOUNDARY = 'BVL_E786_LOCAL_MULTIPART_V1'
SYNTHETIC_TOKEN = 'SYNTHETIC_LOCAL_ONLY_NO_CREDENTIAL'
SYNTHETIC_SIZE = 8 * 1024 * 1024 + 37


class Blocked(RuntimeError):
    pass


def _validate_loopback(base_url: str) -> str:
    u = urlsplit(base_url)
    if (u.scheme != 'http' or u.hostname not in {'127.0.0.1', 'localhost'}
            or u.username or u.password or u.path not in {'', '/'}
            or u.query or u.fragment or u.port is None):
        raise Blocked('SOLO_SERVIDOR_HTTP_LOCAL_PERMITIDO')
    return base_url.rstrip('/')


def build_multipart_request(*, base_url: str, object_name: str, file_bytes: bytes,
                            upload_type: str = 'multipart', if_generation_match: int = 0):
    """Construye peticion inspeccionable. Prohibido enviar a URL Cloud real."""
    _validate_loopback(base_url)
    if upload_type != 'multipart' or type(if_generation_match) is not int or if_generation_match != 0:
        raise Blocked('PROTOCOLO_O_PRECONDICION_INCORRECTOS')
    if not (isinstance(object_name, str) and object_name.startswith('landing/')
            and object_name != 'landing/' and '..' not in object_name.split('/')
            and '\\' not in object_name and '\x00' not in object_name):
        raise Blocked('DESTINO_FUERA_DE_LANDING')
    if not isinstance(file_bytes, bytes) or not file_bytes:
        raise Blocked('CONTENIDO_INVALIDO')

    metadata = json.dumps({'name': object_name, 'contentType': 'application/octet-stream'},
                          sort_keys=True, separators=(',', ':')).encode('utf-8')
    delim = BOUNDARY.encode('ascii')
    body = (b'--' + delim + b'\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n'
            + metadata + b'\r\n--' + delim
            + b'\r\nContent-Type: application/octet-stream\r\n\r\n'
            + file_bytes + b'\r\n--' + delim + b'--\r\n')
    url = (f'{base_url}/upload/storage/v1/b/{quote(BUCKET, safe="")}/o'
           '?uploadType=multipart&ifGenerationMatch=0')
    return Request(url=url, data=body,
                   headers={'Content-Type': f'multipart/related; boundary={BOUNDARY}',
                            'Authorization': f'Bearer {SYNTHETIC_TOKEN}'}, method='POST')


def send_to_loopback_only(req: Request) -> str:
    """Falla cerrado si hay 412, respuesta incierta o session_uri/Location."""
    _validate_loopback(f'{urlsplit(req.full_url).scheme}://{urlsplit(req.full_url).netloc}')
    u = urlsplit(req.full_url)
    q = parse_qs(u.query)
    if (req.get_method() != 'POST' or q != {'uploadType': ['multipart'],
                                          'ifGenerationMatch': ['0']}
            or u.path != f'/upload/storage/v1/b/{BUCKET}/o'):
        raise Blocked('PROTOCOLO_HTTP_NO_AUTORIZADO')
    try:
        with urlopen(req, timeout=10) as response:
            if response.status not in (200, 201) or response.headers.get('Location'):
                raise Blocked('RESPUESTA_NO_CONFIRMADA_O_SESSION_URI')
            parsed = json.loads(response.read(4096).decode('utf-8'))
            if not isinstance(parsed, dict) or not parsed.get('generation'):
                raise Blocked('GENERACION_NO_CONFIRMADA')
            return 'HTTP_MULTIPART_201_LOCAL_CONFIRMADO_SIN_SESSION_URI'
    except Blocked:
        raise
    except HTTPError as e:
        raise Blocked('CONFLICTO_412_NO_REINTENTAR' if e.code == 412 else
                      'ERROR_HTTP_FAIL_CLOSED') from e
    except (URLError, TimeoutError, ValueError, OSError) as e:
        raise Blocked('RESPUESTA_INCIERTA_FAIL_CLOSED') from e


class LocalCaptureHandler(BaseHTTPRequestHandler):
    """Servidor de laboratorio que solo responde en memoria, no escribe Cloud."""
    def do_POST(self):
        size = int(self.headers.get('Content-Length', '0'))
        query = parse_qs(urlsplit(self.path).query)
        if size < 1 or size > 16 * 1024 * 1024:
            self.send_error(413)
            return
        body = self.rfile.read(size)
        self.server.received.append({
            'method': 'POST', 'path': self.path,
            'query': query, 'content_type': self.headers.get('Content-Type'),
            'body_size': size, 'body_sha256': hashlib.sha256(body).hexdigest(),
            'body': body,
        })
        status = getattr(self.server, 'synthetic_status', 201)
        self.send_response(status)
        if getattr(self.server, 'synthetic_location', False):
            self.send_header('Location', '/resumable/session/DEBE_RECHAZARSE')
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'generation': '1001'}).encode('utf-8'))

    def log_message(self, *args):
        return  # No loguear Authorization siquiera sintético


def start_server(*, synthetic_status=201, synthetic_location=False):
    server = ThreadingHTTPServer(('127.0.0.1', 0), LocalCaptureHandler)
    server.received = []
    server.synthetic_status = synthetic_status
    server.synthetic_location = synthetic_location
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def demonstration():
    server, thread = start_server()
    try:
        raw = b'X' * SYNTHETIC_SIZE
        req = build_multipart_request(base_url=f'http://127.0.0.1:{server.server_port}',
                                      object_name=DESTINATION, file_bytes=raw)
        result = send_to_loopback_only(req)
        capture = server.received[0]
        assert result.startswith('HTTP_MULTIPART_201')
        assert capture['body_size'] > 8 * 1024 * 1024
        assert capture['query'] == {'uploadType': ['multipart'], 'ifGenerationMatch': ['0']}
        assert b'X' * 100 in capture['body']
        print('PROTOCOLO_HTTP:', 'POST_UPLOADTYPE_MULTIPART_IFGENERATIONMATCH_0')
        print('ARCHIVO_SINTETICO_BYTES:', len(raw))
        print('CUERPO_HTTP_BYTES:', capture['body_size'])
        print('SESSION_URI:', 'NINGUNA')
        print('RESULTADO:', result)
        print('OPERACIONES_GCP:', 0)
        print('MOTOR_PRODUCTIVO_MODIFICADO:', False)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


if __name__ == '__main__':
    demonstration()
