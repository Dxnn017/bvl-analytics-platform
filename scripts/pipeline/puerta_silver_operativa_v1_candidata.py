#!/usr/bin/env python3
"""BVL / Fase 5. Puerta operativa candidata SIN CLI ni inicio de Spark.

Aplica ratificaciones Quality 26-11-2026. No existe autorizacion valida hasta
recibir anexo, cierre, autorizacion firmados y referencias independientes.
Esta biblioteca depende de los modulos ya versionados en pipeline-daniela.
"""
from __future__ import annotations
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from weakref import WeakKeyDictionary

import adaptador_gcs_silver_v1 as gcs
import puerta_silver_v1_candidata as core

CONTRACT_SHA256 = '6f33f3a88e04b38a5e75863d3ccc67005231f05d474b9916d34fd7030edef20a'
QUALITY_PIN = '5c70c40088a680cfd7ced1859c4d6b335652d96991cdcae4268819fb4f90f5ce'
ANNEX_SCHEMA = 'BVL-SILVER-OPERATIONAL-ANNEX-v1'  # PROPUESTA pendiente emision Quality
CLOSURE_SCHEMA = 'BVL-PHASE5-CLOSURE-v1'
AUTH_SCHEMA = 'BVL-SILVER-AUTH-v1'
CONTROL = 'bvl26-control-7f3a9c'
SILVER = 'bvl26-silver-7f3a9c'
RUN_PATTERN = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_-]{0,90}$')
PRODUCT_PATTERN = re.compile(r'^[a-z0-9_]+(?:/[a-z0-9_]+)+$')
TOKEN = object()
# Registro en memoria: solamente un preflight GCS completo emite capacidades.
_GCS_ISSUED_PLANS = WeakKeyDictionary()

class OperationalError(ValueError):
    pass

def must(ok, msg):
    if not ok: raise OperationalError(msg)

def canonical(obj):
    return core.canonical_bytes(obj)

def digest(raw):
    return hashlib.sha256(raw).hexdigest()

def signed(raw, signature, pem):
    """Valida canonicalizacion exacta, firma DER P256 y pin EXTERNO fijo.

    La constante QUALITY_PIN no se lee de los objetos recibidos.
    """
    return core.verify_signed_authorization(raw, signature, pem, expected_pin=QUALITY_PIN)

def check_base(raw):
    # El contrato cerrado historicamente en Fase 5 no se altera.
    return core.validate_contract(raw, CONTRACT_SHA256)

def same_context(obj, contract):
    for k in ('run_id','project_id','region'):
        must(obj.get(k) == contract.get(k), 'Contexto de ' + k + ' distinto')
    must(obj.get('base_contract_sha256') == CONTRACT_SHA256, 'Contrato base no vinculado')

def valid_product(name):
    must(isinstance(name,str) and PRODUCT_PATTERN.fullmatch(name) is not None, 'Producto invalido')
    return name

def distinct(items, what):
    must(len(set(items)) == len(items), 'Elementos duplicados: ' + what)

def validate_annex(a, c):
    must(a.get('schema_version') == ANNEX_SCHEMA, 'Esquema anexo no ratificado')
    same_context(a,c)
    must(a.get('status') == 'RATIFICADO' and a.get('quality_approved') is True, 'Anexo sin aprobacion Quality')
    direct = a.get('direct_processes')
    must(isinstance(direct,list) and len(direct) == 12, 'Se requieren 12 procesamientos Bronze')
    names=[p.get('bronze_dataset') for p in direct]
    distinct(names, 'datasets')
    must(set(names) == {d['dataset'] for d in c['datasets']}, 'Datasets incompletos')
    outputs=[]
    for p in direct:
        products=p.get('silver_products')
        must(isinstance(products,list) and bool(products), 'Sin salidas Silver')
        distinct(products, 'salidas')
        for name in products: valid_product(name)
        ds=next(d for d in c['datasets'] if d['dataset']==p['bronze_dataset'])
        if ds.get('proposed_silver_product'):
            must(ds['proposed_silver_product'] in products, 'Mapeo distinto al contrato')
        outputs += products
    distinct(outputs,'productos directos')
    by_source={p['bronze_dataset']: p['silver_products'] for p in direct}
    must(set(by_source['smv/valores_inscritos']) == {
        'referencias_smv/valores_inscritos','referencias_smv/puente_ruc_nemonico'},
        'Valores inscritos requiere sus dos salidas')
    must(by_source['smv/hechos_importancia']==['hechos_importancia/eventos'], 'Mapeo Hechos distinto')
    must(by_source['smv/hechos_valores_consulta']==['referencias_smv/hechos_valores_consulta'], 'Mapeo consulta distinto')
    derived=a.get('derived_processes')
    must(isinstance(derived,list) and len(derived)==1, 'Derivados deben ser explicitados')
    item=derived[0]
    must(item.get('silver_product')=='hechos_importancia/identidad_empresarial', 'Derivado no reconocido')
    deps=item.get('silver_dependencies')
    expected={
        'hechos_importancia/eventos',
        'finanzas_empresariales/principales_cuentas',
        'referencias_smv/puente_ruc_nemonico',
        'mercado_diario/cotizaciones',
        'referencias_smv/hechos_valores_consulta'
    }
    must(isinstance(deps,list) and set(deps)==expected and len(deps)==len(expected), 'Dependencias de identidad diferentes')
    must(item.get('bronze_dependencies')==[], 'Bronze extra sin justificacion')
    must(a.get('automatic_retry_allowed') is False and a.get('overwrite_allowed') is False,
         'Anexo permite operaciones inseguras')
    return a

def validate_closure(x, c, annex_raw):
    must(x.get('schema_version') == CLOSURE_SCHEMA, 'Esquema acta no reconocido')
    same_context(x,c)
    must(x.get('status') == 'PHASE5_CLOSED' and x.get('phase_5_closed') is True
         and x.get('quality_approved') is True, 'Fase 5 no cerrada por Quality')
    must(x.get('operational_annex_sha256') == digest(annex_raw), 'Acta no corresponde al anexo')
    core.utc(x.get('closed_at_utc'))
    return x

def stage(product, run_id, attempt):
    return f'gs://{SILVER}/staging/{product}/run_id={run_id}/attempt_id={attempt}/'

def authorization_paths(mode, dataset, product, run_id, attempt):
    must(RUN_PATTERN.fullmatch(attempt) is not None, 'ID de intento invalido')
    if mode == 'SINGLE_SOURCE':
        must(isinstance(dataset,str) and dataset, 'Dataset requerido')
        base=f'authorizations/silver/{dataset}/run_id={run_id}/attempt_id={attempt}/'
    elif mode == 'MULTI_SOURCE':
        valid_product(product)
        base=f'authorizations/silver/derived/{product}/run_id={run_id}/attempt_id={attempt}/'
    else: raise OperationalError('Modalidad desconocida')
    return f'gs://{CONTROL}/{base}authorization.json', f'gs://{CONTROL}/{base}authorization.sig'

def validate_auth(x, c, a, annex_raw, closure_raw, now=None):
    now=now or datetime.now(timezone.utc)
    must(x.get('schema_version')==AUTH_SCHEMA and x.get('status')=='SILVER_AUTORIZADO', 'Autorizacion inexistente')
    same_context(x,c)
    must(x.get('operational_annex_sha256')==digest(annex_raw), 'Auth no enlazada al anexo')
    must(x.get('phase5_closure_sha256')==digest(closure_raw), 'Auth no enlazada al acta')
    must(x.get('operation')=='BUILD_SILVER_STAGING', 'Operacion no permitida')
    must(x.get('service_account')==c['consumer']['service_account'], 'Identidad distinta')
    must(x.get('overwrite_allowed') is False and x.get('official_publication_allowed') is False
         and x.get('automatic_retry_allowed') is False, 'Permiso inseguro')
    if x.get('recovery_of_fingerprint') is not None:
        must(core.HEX64.fullmatch(x['recovery_of_fingerprint']) is not None,
             'Recuperacion: fingerprint anterior invalido')
        must(isinstance(x.get('quality_reconciliation_id'),str) and x['quality_reconciliation_id'],
             'Recuperacion sin conciliacion Quality')
        must(isinstance(x.get('previous_attempt_id'),str) and x['previous_attempt_id']
             and x['previous_attempt_id']!=x.get('attempt_id'),
             'Recuperacion reutiliza el intento anterior')
    attempt=x.get('attempt_id')
    must(isinstance(attempt,str) and RUN_PATTERN.fullmatch(attempt) is not None, 'Intento no valido')
    start,end=core.utc(x.get('not_before_utc')),core.utc(x.get('expires_at_utc'))
    must(0<(end-start).total_seconds()<=14400 and start<=now<=end, 'Vigencia o duracion invalida')
    mode=x.get('mode')
    bron=x.get('bronze_dependencies')
    silver=x.get('silver_dependencies')
    must(isinstance(bron,list) and isinstance(silver,list), 'Dependencias incompletas')
    ds_by_name={d['dataset']:d for d in c['datasets']}
    actual_bronze=[]
    for d in bron:
        must(isinstance(d,dict) and d.get('dataset') in ds_by_name, 'Bronze no reconocido')
        expected=ds_by_name[d['dataset']]
        must(d.get('manifest_uri')==expected['bronze_manifest_uri'] and
             str(d.get('manifest_generation'))==str(expected['bronze_manifest_generation']) and
             d.get('manifest_sha256')==expected['bronze_manifest_sha256'], 'Manifest Bronze distinto')
        actual_bronze.append(d['dataset'])
    distinct(actual_bronze,'Bronze deps')
    actual_silver=[]
    for d in silver:
        must(isinstance(d,dict), 'Silver dep invalida')
        product=valid_product(d.get('product'))
        must(isinstance(d.get('version_id'),str) and RUN_PATTERN.fullmatch(d['version_id']) is not None,
             'Version Silver no fijada')
        uri=d.get('manifest_uri')
        must(isinstance(uri,str) and uri.startswith(f'gs://{CONTROL}/manifests/silver/')
             and '/staging/' not in uri and '/latest/' not in uri and '/landing/' not in uri,
             'Manifest Silver no oficial')
        gcs.positive_generation(d.get('manifest_generation'))
        must(isinstance(d.get('manifest_sha256'),str) and core.HEX64.fullmatch(d['manifest_sha256']) is not None,
             'SHA Silver no valido')
        actual_silver.append(product)
    distinct(actual_silver,'Silver deps')
    if mode=='SINGLE_SOURCE':
        must(len(actual_bronze)==1 and silver==[], 'SINGLE_SOURCE requiere un solo Bronze')
        must(x.get('dataset')==actual_bronze[0], 'Dataset no corresponde a dependencia')
        must(x.get('silver_product') is None, 'Producto derivado no permitido en SINGLE_SOURCE')
        expected=next(p['silver_products'] for p in a['direct_processes'] if p['bronze_dataset']==x['dataset'])
    elif mode=='MULTI_SOURCE':
        must(x.get('dataset') is None, 'MULTI_SOURCE no es autorizacion de dataset simple')
        prod=valid_product(x.get('silver_product'))
        p=next((p for p in a['derived_processes'] if p['silver_product']==prod),None)
        must(p is not None,'Derivado no ratificado')
        must(set(actual_bronze)==set(p['bronze_dependencies']) and
             set(actual_silver)==set(p['silver_dependencies']), 'Dependencias multiples incompletas')
        must(len(actual_bronze)+len(actual_silver)>=2, 'MULTI_SOURCE requiere varias fuentes')
        expected=[prod]
    else: raise OperationalError('Modalidad no reconocida')
    outs=x.get('silver_outputs')
    must(isinstance(outs,list) and len(outs)==len(expected), 'No estan todas las salidas')
    names=[]
    for o in outs:
        must(isinstance(o,dict),'Salida invalida')
        name=valid_product(o.get('name'))
        must(o.get('staging_prefix')==stage(name,c['run_id'],attempt), 'Destino staging fuera del intento')
        names.append(name)
    must(set(names)==set(expected) and len(names)==len(expected), 'Productos no ratificados/faltantes')
    return x

def fingerprint(auth):
    """Huella SEMANTICA del alcance firmado e intento; nunca depende de expiry.

    Orden de arreglos y nueva vigencia no deben permitir reutilizar el intento.
    """
    scope={k:auth[k] for k in (
        'schema_version','mode','run_id','attempt_id','operation','service_account',
        'project_id','region','base_contract_sha256','operational_annex_sha256',
        'phase5_closure_sha256')}
    scope['dataset']=auth.get('dataset')
    scope['silver_product']=auth.get('silver_product')
    scope['silver_outputs']=sorted(auth['silver_outputs'],key=lambda x:x['name'])
    scope['bronze_dependencies']=sorted(auth['bronze_dependencies'],key=lambda x:x['dataset'])
    scope['silver_dependencies']=sorted(auth['silver_dependencies'],key=lambda x:x['product'])
    return digest(canonical(scope))

@dataclass(frozen=True)
class VerifiedPlan:
    _proof: object
    authorization: dict
    fingerprint: str
    consume_uri: str
    checked_at: datetime


@dataclass(frozen=True, eq=False, slots=True, weakref_slot=True)
class GCSVerifiedPlan:
    """Capacidad temporal emitida SOLAMENTE tras preflight read-only completo GCS.

    La autorización se guarda como bytes canónicos inmutables. `authorization`
    devuelve una COPIA, para impedir mutaciones del alcance firmado.
    Un VerifiedPlan offline NO pertenece a este tipo y nunca habilita claim.
    """
    _proof: object
    _auth_raw: bytes
    fingerprint: str
    consume_uri: str
    checked_at: datetime
    _client: object
    _principal: str

    @property
    def authorization(self):
        return core.load_strict(self._auth_raw)


def effective_identity_from_client(client):
    """Identidad de credenciales vinculadas al cliente GCS que lee los datos.

    No se recibe principal por argumento. En despliegue real, Quality debe
    verificar que las credenciales ADC/runtime reflejen la SA efectiva y
    las políticas IAM del entorno; si la identidad no es legible, FALLA cerrado.
    """
    credentials = getattr(client, '_credentials', None)
    email = getattr(credentials, 'service_account_email', None)
    must(isinstance(email, str) and '@' in email and email.strip() == email,
         'Identidad efectiva GCS no verificable desde las credenciales')
    return email


def validate_signed_bundle(contract_raw, annex_raw, annex_sig, closure_raw, closure_sig,
                           auth_raw, auth_sig, trusted_quality_pem, now=None):
    """Validacion pura en memoria. NO lee GCS ni crea claim."""
    now=now or datetime.now(timezone.utc)
    c=check_base(contract_raw)
    a=validate_annex(signed(annex_raw,annex_sig,trusted_quality_pem),c)
    closure=validate_closure(signed(closure_raw,closure_sig,trusted_quality_pem),c,annex_raw)
    auth=validate_auth(signed(auth_raw,auth_sig,trusted_quality_pem),c,a,annex_raw,closure_raw,now=now)
    fp=fingerprint(auth)
    path=f'gs://{CONTROL}/authorizations/consumed/silver/run_id={c["run_id"]}/{fp}.json'
    return VerifiedPlan(TOKEN,auth,fp,path,now)

def check_gcs_signed_bundle(client, trusted_refs, trusted_pem, contract_raw, now=None):
    """Read-only. `trusted_refs` debe venir de contexto Quality independiente.

    Los campos auth/annex/closure de `trusted_refs` contienen URI, generacion,
    SHA256 y referencias independientes de firma y su version. NO extraerlos
    del mismo JSON de autorizacion. NO SE INVOCA por CLI ni automaticamente.
    """
    must(isinstance(trusted_refs,dict) and set(trusted_refs)=={'annex','closure','authorization'},
         'Contexto de entrega de Quality incompleto')
    doc={}
    for typ,refs in trusted_refs.items():
        must(isinstance(refs,dict),'Referencia externa invalida')
        uri=refs.get('uri'); s_uri=refs.get('signature_uri')
        bucket, key=gcs.split_gs_uri(uri); sb, sk=gcs.split_gs_uri(s_uri)
        must(bucket==CONTROL and sb==CONTROL, 'Artefactos fuera de Control')
        must(sk==key.rsplit('/',1)[0]+'/'+key.rsplit('/',1)[1].replace('.json','.sig'),
             'Firma no esta junto al documento')
        if typ=='authorization':
            must(key.startswith('authorizations/silver/'), 'Autorizacion fuera de ruta')
        else:
            must(key.startswith('contracts/bronze_silver/') or key.startswith('authorizations/phase5/'),
                 'Anexo/acta fuera de prefijo de Control propuesto')
        doc[typ]=(gcs.read_exact_current_bytes(client, uri,refs['generation'],refs['sha256'],allowed_bucket=CONTROL),
                  gcs.read_exact_current_bytes(client,s_uri,refs['signature_generation'],
                                               refs['signature_sha256'], allowed_bucket=CONTROL))
    plan=validate_signed_bundle(contract_raw,*doc['annex'],*doc['closure'],*doc['authorization'],trusted_pem,now)
    c=check_base(contract_raw)
    x=plan.authorization
    au,sg=authorization_paths(x['mode'],x.get('dataset'),x.get('silver_product'),c['run_id'],x['attempt_id'])
    must(trusted_refs['authorization']['uri']==au and trusted_refs['authorization']['signature_uri']==sg,
         'Ruta de auth no coincide con modalidad/intento')
    principal = effective_identity_from_client(client)
    must(principal == x['service_account'],
         'Identidad efectiva del cliente GCS distinta de la autorizacion firmada')
    # Read-only: verificar manifests y bytes de todas las entradas ANTES de claim.
    for dep in x['bronze_dependencies']:
        ds=next(d for d in c['datasets'] if d['dataset']==dep['dataset'])
        _, files=gcs.read_official_manifest(client, ds,core.verify_manifest)
        gcs.verify_official_inputs(client,files)
    for dep in x['silver_dependencies']:
        raw=gcs.read_exact_current_bytes(client,dep['manifest_uri'],dep['manifest_generation'],
                                         dep['manifest_sha256'],allowed_bucket=CONTROL)
        m=core.load_strict(raw)
        must(m.get('product')==dep['product'] and m.get('version_id')==dep['version_id']
             and m.get('quality_approved') is True and m.get('status')=='SILVER_PUBLICADO',
             'Dependencia Silver no es oficial')
        # Referencias de archivos oficiales Silver; el contrato de publicacion
        # Silver debe ratificar este formato antes de la primera MULTI_SOURCE.
        files=m.get('files')
        must(isinstance(files,list) and files, 'Manifest Silver sin archivos oficiales')
        seen=set()
        for f in files:
            must(isinstance(f,dict), 'Archivo Silver invalido')
            uri=f.get('uri')
            must(isinstance(uri,str) and uri.startswith(f'gs://{SILVER}/')
                 and '/staging/' not in uri and '/landing/' not in uri,
                 'Archivo Silver no publicado')
            must(uri not in seen, 'Silver duplicado')
            seen.add(uri)
            gcs.read_exact_current_bytes(client, uri, f.get('generation'),
                f.get('sha256'), expected_size=f.get('size_bytes'),allowed_bucket=SILVER)
    # La frescura se mide desde el FIN del preflight, no desde el comienzo:
    # verificar muchos objetos Bronze puede tomar mas de 120 segundos.
    completed_at = now if now is not None else datetime.now(timezone.utc)
    start, end = core.utc(x['not_before_utc']), core.utc(x['expires_at_utc'])
    must(start <= completed_at <= end, 'Autorizacion expiro durante el preflight GCS')
    issued = GCSVerifiedPlan(TOKEN, canonical(x), plan.fingerprint,
                             plan.consume_uri, completed_at, client, principal)
    _GCS_ISSUED_PLANS[issued] = (plan.fingerprint, client, principal)
    return issued

def consume_once(plan, claim_store, *, now=None):
    """UNICA escritura GCS. NO llamar hasta Quality autorice acto supervisado.

    No expone CLI; el llamador necesita usar plan verificado inmediatamente.
    La condicion GCS if_generation_match=0 la aplica GCSExclusiveClaimStore.
    """
    must(type(plan) is GCSVerifiedPlan and plan._proof is TOKEN,
         'Plan no verificado operativamente en GCS (offline no puede consumir)')
    must(_GCS_ISSUED_PLANS.get(plan) == (plan.fingerprint, plan._client, plan._principal),
         'Plan GCS sin prueba de emision desde preflight completo')
    must(getattr(claim_store, 'client', None) is plan._client,
         'El claim debe usar el mismo cliente GCS del preflight')
    auth=plan.authorization  # Copia del alcance canonicalizado durante preflight
    must(fingerprint(auth)==plan.fingerprint,
         'Fingerprint no corresponde al alcance firmado')
    expected_uri=(f'gs://{CONTROL}/authorizations/consumed/silver/'
                  f'run_id={auth["run_id"]}/{plan.fingerprint}.json')
    must(plan.consume_uri==expected_uri, 'URI de consumo no vinculada al fingerprint')
    must(effective_identity_from_client(plan._client)==plan._principal==auth['service_account'],
         'Identidad efectiva no coincide con credenciales usadas en preflight')
    now=now or datetime.now(timezone.utc)
    start,end=core.utc(auth['not_before_utc']),core.utc(auth['expires_at_utc'])
    age=(now-plan.checked_at).total_seconds()
    must(start<=now<=end and 0<=age<=120,
         'Plan expirado: repetir preflight GCS antes de consumir')
    payload=canonical({'schema_version':'BVL-SILVER-CLAIM-v1','status':'CONSUMED',
                      'fingerprint':plan.fingerprint,'run_id':auth['run_id'],
                      'attempt_id':auth['attempt_id'],
                      'authorization_sha256':digest(plan._auth_raw),
                      'created_at_utc':now.isoformat()})
    return claim_store.create_exclusive(plan.consume_uri,payload)


def validate_joint_staging_completion(verified_plan, attestations):
    """Criterio de cierre de staging: TODAS las salidas, o ninguna aprobada.

    `attestations` deben provenir del verificador de staging/Quality, NO del
    propio Spark. NO publica Silver ni cambia el claim.
    """
    must(type(verified_plan) in (VerifiedPlan, GCSVerifiedPlan) and verified_plan._proof is TOKEN,
         'Plan de cierre no verificado')
    expected={o['name']:o['staging_prefix'] for o in
              verified_plan.authorization['silver_outputs']}
    must(isinstance(attestations,list) and len(attestations)==len(expected),
         'Falta una o mas salidas de staging')
    reported={}
    for v in attestations:
        must(isinstance(v,dict) and v.get('name') in expected, 'Salida no autorizada')
        name=v['name']
        must(name not in reported, 'Salida staging duplicada')
        must(v.get('staging_prefix')==expected[name], 'Staging distinto al autorizado')
        must(v.get('status')=='STAGING_VALIDADO' and v.get('quality_check_passed') is True,
             'Staging sin validacion')
        must(isinstance(v.get('files_count'),int) and v['files_count']>0,
             'Staging vacio')
        reported[name]=True
    must(set(reported)==set(expected), 'Faltan salidas')
    return {'status':'STAGING_COMPLETO_PENDIENTE_PUBLICACION',
            'outputs_checked':len(reported),'official_publication_allowed':False}
