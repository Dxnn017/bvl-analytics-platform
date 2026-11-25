import copy
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts' / 'ingestion'))
from preparar_piloto_bronze_local import validar_plan, preparar, guardar_sin_sobrescribir, PreparacionError

RUN = 'bvl-bronze-inicial-20261124-001'
DATASETS = ('bcrp/embig', 'bcrp/tipo_cambio', 'smv/cambios_patrimonio',
            'smv/cotizaciones', 'smv/estado_resultados', 'smv/flujo_efectivo',
            'smv/hechos_importancia', 'smv/hechos_valores_consulta',
            'smv/montos_sab', 'smv/principales_cuentas',
            'smv/situacion_financiera', 'smv/valores_inscritos')

def plan_valido():
    ops = []
    for i in range(3599):
        ds = DATASETS[i % 12]
        relativo = f'ingestion_date=2026-11-24/file_{i:04d}.json'
        ops.append({'run_id': RUN, 'dataset': ds,
                    'origen': f'/datalake/bvl/bronze/{ds}/{relativo}',
                    'destino': f'gs://bvl26-bronze-7f3a9c/landing/{ds}/run_id={RUN}/{relativo}',
                    'size_bytes': 362240 if i == 0 else 109300,
                    'sha256': f'{i+1:064x}', 'estado': 'PLANIFICADO_NO_EJECUTADO'})
    return {'run_id': RUN, 'tipo': 'PLAN_LOCAL_BRONZE', 'publicacion': 'NO_PUBLICADO',
            'transferencias': 0, 'datasets': 12, 'archivos': 3599,
            'bytes': 393623640, 'operaciones': ops}

class PruebasPilotajeLocal(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = plan_valido()
    def test_piloto_valido_seleccion_12_datasets(self):
        p = preparar(self.base, 'a'*64)
        self.assertEqual(p['archivos'], 12)
        self.assertEqual(len(set(x['dataset'] for x in p['archivos_piloto'])), 12)
        self.assertEqual(p['transferencias_cloud'], 0)
        self.assertFalse(p['transfer_allowed'])
        self.assertEqual(p['bytes_esperados'], 12*109300)
        self.assertTrue(all(x['estado']=='PENDIENTE_AUTORIZACION' for x in p['archivos_piloto']))
    def test_carpeta_pdf_antes_de_ingestion_date(self):
        p = copy.deepcopy(self.base)
        op = p['operaciones'][6]
        op['origen'] = op['origen'].replace(
            '/ingestion_date=', '/pdf/ingestion_date=', 1)
        op['destino'] = op['destino'].replace(
            '/ingestion_date=', '/pdf/ingestion_date=', 1)
        validar_plan(p)

    def test_fecha_ausente_rechazada(self):
        p = copy.deepcopy(self.base)
        op = p['operaciones'][6]
        op['origen'] = op['origen'].replace('ingestion_date=', 'fecha=', 1)
        op['destino'] = op['destino'].replace('ingestion_date=', 'fecha=', 1)
        with self.assertRaisesRegex(PreparacionError, 'OP_INGESTION_DATE'):
            validar_plan(p)

    def test_fecha_calendario_invalida_rechazada(self):
        p = copy.deepcopy(self.base)
        op = p['operaciones'][6]
        op['origen'] = op['origen'].replace('2026-11-24', '2026-02-30', 1)
        op['destino'] = op['destino'].replace('2026-11-24', '2026-02-30', 1)
        with self.assertRaisesRegex(
            PreparacionError, 'OP_INGESTION_DATE_CALENDARIO'
        ):
            validar_plan(p)

    def test_registro_operativo_falso_rechazado(self):
        p = copy.deepcopy(self.base)
        p['transferencias'] = 1
        with self.assertRaisesRegex(PreparacionError,'PLAN_NO_LOCAL'): validar_plan(p)
    def test_destino_mutado_rechazado(self):
        p = copy.deepcopy(self.base)
        p['operaciones'][3]['destino']='gs://otro/landing/'
        with self.assertRaisesRegex(PreparacionError,'OP_DESTINO'): validar_plan(p)
    def test_traversal_rechazado(self):
        p = copy.deepcopy(self.base)
        p['operaciones'][3]['origen'] += '/../secreto'
        p['operaciones'][3]['destino'] += '/../secreto'
        with self.assertRaisesRegex(PreparacionError,'OP_TRAVERSAL'): validar_plan(p)
    def test_sha_mutado_rechazado(self):
        p = copy.deepcopy(self.base)
        p['operaciones'][3]['sha256']='x'*64
        with self.assertRaisesRegex(PreparacionError,'OP_SHA'): validar_plan(p)
    def test_duplicado_rechazado(self):
        p = copy.deepcopy(self.base)
        p['operaciones'][4]['origen']=p['operaciones'][16]['origen']
        p['operaciones'][4]['destino']=p['operaciones'][16]['destino']
        with self.assertRaisesRegex(PreparacionError,'OP_DUPLICADO'): validar_plan(p)
    def test_idempotencia_y_contenido_estable(self):
        p = preparar(self.base,'b'*64)
        with tempfile.TemporaryDirectory() as d:
            r1 = guardar_sin_sobrescribir(Path(d),p)
            r2 = guardar_sin_sobrescribir(Path(d),p)
            self.assertEqual(r1,r2)
            self.assertEqual(len(r1),3)
            with open(r1[0],encoding='utf-8') as f:
                self.assertEqual(json.load(f)['estado'],'CANDIDATO_NO_AUTORIZADO')
            with open(r1[2],encoding='utf-8') as f:
                self.assertIn(hashlib.sha256(Path(r1[0]).read_bytes()).hexdigest(),f.read())
    def test_conflicto_no_sobrescribe(self):
        p = preparar(self.base,'b'*64)
        with tempfile.TemporaryDirectory() as d:
            r = guardar_sin_sobrescribir(Path(d),p)
            Path(r[1]).write_text('contenido alterado')
            with self.assertRaisesRegex(PreparacionError,'CONFLICTO_ARCHIVO_EXISTENTE'):
                guardar_sin_sobrescribir(Path(d),p)
            self.assertEqual(Path(r[1]).read_text(),'contenido alterado')

if __name__ == '__main__': unittest.main()
