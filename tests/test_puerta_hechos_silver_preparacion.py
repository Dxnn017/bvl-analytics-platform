import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
MOD_PATH=ROOT/'scripts/pipeline/puerta_hechos_silver_preparacion.py'
SPE=importlib.util.spec_from_file_location('gate_hechos_test',MOD_PATH)
gate=importlib.util.module_from_spec(SPE)
SPE.loader.exec_module(gate)
CONTRACT=ROOT/'fixtures/contrato_bronze_silver_draft_para_pruebas.json'


def make_fixture(tmp):
    stage=Path(tmp)/'staging';stage.mkdir()
    ds='smv/hechos_valores_consulta'
    pref=f'gs://bvl26-bronze-7f3a9c/{ds}/run_id=bvl-bronze-inicial-20261125-001/'
    data={'Resultado':[],'relleno':''}
    unpadded=len(json.dumps(data,ensure_ascii=False,separators=(',',':')).encode('utf-8'))
    pad=19917-(22*4)-unpadded
    data['relleno']='x'*pad
    json_bytes=json.dumps(data,ensure_ascii=False,separators=(',',':')).encode('utf-8')
    assert len(json_bytes)==19917-22*4
    files=[];entries=[]
    specs=[('json/ingestion_date=2026-10-05/hechos_empresas_valores_smv.json',json_bytes)]
    specs += [(f'xml/ingestion_date=2026-10-05/{i:02d}.xml',b'<r/>') for i in range(22)]
    for suffix,blob in specs:
        path=stage/Path(suffix).name
        path.write_bytes(blob)
        uri=pref+suffix;g='1234567890'
        files.append(dict(uri=uri,generation=g,size_bytes=len(blob),sha256=hashlib.sha256(blob).hexdigest()))
        entries.append(dict(uri=uri,generation=g,local_path=str(path)))
    manifest=dict(run_id='bvl-bronze-inicial-20261125-001',dataset=ds,files=files,status='BORRADOR_NO_PUBLICAR')
    return stage,stage/'hechos_empresas_valores_smv.json',manifest,entries

class GateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contrato=json.loads(CONTRACT.read_text(encoding='utf-8'))
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.stage,self.json_path,self.manifest,self.entries=make_fixture(self.temp.name)
    def tearDown(self): self.temp.cleanup()
    def run_gate(self):
        return gate.validar_preparacion(self.contrato,self.manifest,self.entries,self.stage,self.json_path)
    def test_accept_readonly_and_deny_authorization(self):
        r=self.run_gate();self.assertEqual(r['archivos'],23);self.assertEqual(r['bytes_verificados'],19917)
        for f in ('autoriza_silver','manifest_oficial_autenticado','generaciones_gcs_verificadas','ejecuta_spark'):
            self.assertIs(r[f],False)
    def test_different_dataset(self):
        self.manifest['dataset']='smv/cotizaciones'
        with self.assertRaises(ValueError): self.run_gate()
    def test_missing_file(self):
        self.entries.pop()
        with self.assertRaises(ValueError): self.run_gate()
    def test_altered_file(self):
        self.json_path.write_bytes(self.json_path.read_bytes()+b'x')
        with self.assertRaises(ValueError):self.run_gate()
    def test_altered_manifest_hash(self):
        self.manifest['files'][0]['sha256']='0'*64
        with self.assertRaises(ValueError):self.run_gate()
    def test_wrong_generation(self):
        self.entries[0]['generation']='123'
        with self.assertRaises(ValueError):self.run_gate()
    def test_wrong_canonical_run(self):
        self.manifest['run_id']='otro'
        with self.assertRaises(ValueError):self.run_gate()
    def test_missing_xml(self):
        self.manifest['files'].pop()
        with self.assertRaises(ValueError):self.run_gate()
    def test_different_size(self):
        self.manifest['files'][0]['size_bytes']+=1
        with self.assertRaises(ValueError):self.run_gate()
    def test_json_is_invalid(self):
        self.json_path.write_bytes(b'A'*self.json_path.stat().st_size)
        self.manifest['files'][0]['sha256']=hashlib.sha256(self.json_path.read_bytes()).hexdigest()
        with self.assertRaises(ValueError):self.run_gate()
    def test_bronze_json_path_not_bound(self):
        with self.assertRaises(ValueError):
            gate.validar_preparacion(self.contrato,self.manifest,self.entries,self.stage,self.stage/'00.xml')
    def test_staging_root_invalid(self):
        with self.assertRaises(ValueError):
            gate.validar_preparacion(self.contrato,self.manifest,self.entries,self.stage/'none',self.json_path)
    def test_reject_draft_becoming_authorized(self):
        self.contrato2=json.loads(json.dumps(self.contrato))
        self.contrato2['silver_consumption_allowed']=True
        with self.assertRaises(ValueError):
            gate.validar_preparacion(self.contrato2,self.manifest,self.entries,self.stage,self.json_path)
    def test_refuse_landing_uri(self):
        self.entries[0]['uri']=self.entries[0]['uri'].replace('/smv/','/landing/smv/')
        with self.assertRaises(ValueError):self.run_gate()
    def test_duplicate_uri(self):
        self.entries[1]['uri']=self.entries[0]['uri']
        with self.assertRaises(ValueError):self.run_gate()
    def test_no_xml_semantic_claim(self):
        r=self.run_gate();self.assertIs(r['conciliacion_semantica_xml_json'],False)
    def test_symlink(self):
        source=Path(self.entries[1]['local_path']);source.unlink();source.symlink_to(self.json_path)
        with self.assertRaises(ValueError): self.run_gate()

class CandidateWiringTests(unittest.TestCase):
    def test_mock_candidate_is_fail_closed(self):
        with tempfile.TemporaryDirectory() as d:
            base=Path(d)
            script=base/'scripts/processing/hechos_identidad_gcp_candidato.py';script.parent.mkdir(parents=True)
            original=base/'scripts/processing/hechos_identidad_to_silver.py'
            original.write_text('print("original")\n')
            script.write_text("\nSILVER_EJECUCION_HABILITADA = False\nif not SILVER_EJECUCION_HABILITADA:\n    raise SystemExit('BLOCKED')\n\n# BVL_CANDIDATO_PARAMETRIZADO_V1\nprint('SPARK_STARTED')\nx=\"errorifexists\"\n")
            target=base/'scripts/pipeline';target.mkdir(parents=True)
            for n in ('preflight_bronze_silver_offline.py','verificar_snapshot_bronze_silver_offline.py','puerta_hechos_silver_preparacion.py'):
                (target/n).write_bytes((ROOT/'scripts/pipeline'/n).read_bytes())
            installer=ROOT/'tools/aplicar_puerta_hechos_silver.py'
            env=os.environ.copy();env['HOME']=str(base)
            (base/'Descargas').mkdir()
            done=subprocess.run([sys.executable,str(installer)],cwd=base,env=env,capture_output=True,text=True)
            self.assertEqual(done.returncode,0,done.stderr+done.stdout)
            proc=subprocess.run([sys.executable,str(script)],cwd=base,capture_output=True,text=True)
            self.assertNotEqual(proc.returncode,0)
            self.assertNotIn('SPARK_STARTED',proc.stdout)
            self.assertIn('BLOCKED',proc.stderr)
            proc2=subprocess.run([sys.executable,str(script),'--preflight-offline','--help'],cwd=base,capture_output=True,text=True)
            self.assertEqual(proc2.returncode,0,proc2.stderr)
            self.assertIn('--manifest-suministrado',proc2.stdout)
            self.assertNotIn('SPARK_STARTED',proc2.stdout)

if __name__=='__main__':unittest.main()
