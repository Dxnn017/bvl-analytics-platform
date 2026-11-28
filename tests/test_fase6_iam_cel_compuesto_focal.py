"""Pruebas focalizadas offline de expresion IAM CEL compuesta de Quality."""
from __future__ import annotations
from pathlib import Path
from unittest import TestCase
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'/'processing'))
sys.path.insert(0,str(ROOT/'tests'))
import fase6_hechos_launcher_documental_v1 as launch
from test_fase6_hechos_launcher_documental_v1 import candidate_context
from test_fase6_iam_atestacion_firmada_v1 import sample_attestation

class TestCELCompuestoFocal(TestCase):
    def setUp(self):
        self.context=candidate_context()
        self.doc=sample_attestation(self.context)
        self.prefix=f'projects/_/buckets/{launch.SILVER_BUCKET}/objects/staging/'

    def expr(self, prefix=None, obj_type='storage.googleapis.com/Object', quotes="'"):
        if prefix is None: prefix=self.prefix
        return (f'resource.type == {quotes}{obj_type}{quotes} && '
                f'resource.name.startsWith({quotes}{prefix}{quotes})')

    def check(self):
        return launch.validate_iam_scope_attestation(self.doc,self.context,'a'*64)

    def put(self,expr):
        self.doc['allow_policy']['bindings'][0]['condition']['expression']=expr

    def blocked(self,expr,reason='IAM_POLICY_NO_ACREDITA_TODOS_LOS_OBJETOS'):
        self.put(expr)
        with self.assertRaisesRegex(ValueError,reason): self.check()

    def test_01_exact_composite_quality_single_quotes(self):
        self.put(self.expr())
        self.assertTrue(self.check())

    def test_02_composite_double_quotes(self):
        self.put(self.expr(quotes='"'))
        self.assertTrue(self.check())

    def test_03_compound_both_quote_types_for_two_literals(self):
        self.put(f'resource.type == "storage.googleapis.com/Object" && '
                 f"resource.name.startsWith('{self.prefix}')")
        self.assertTrue(self.check())

    def test_04_original_simple_startswith_still_accepted(self):
        self.put(f"resource.name.startsWith('{self.prefix}')")
        self.assertTrue(self.check())

    def test_05_wrong_resource_type_blocks(self):
        self.blocked(self.expr(obj_type='storage.googleapis.com/Bucket'))

    def test_06_wrong_bucket_blocks(self):
        self.blocked(self.expr(prefix='projects/_/buckets/otro/objects/staging/'))

    def test_07_prefix_parquet_only_does_not_cover_quality_targets(self):
        self.blocked(self.expr(prefix=self.prefix+'hechos_importancia/documental_restringido/run_id='+self.context['run_id']+'/attempt_id='+self.context['attempt_id']+'/parquet/'))

    def test_08_probe_prefix_does_not_cover_ten_targets(self):
        self.blocked(self.expr(prefix=self.prefix+'hechos_importancia/documental_restringido/_iam_diagnostic_only/'))

    def test_09_arbitrary_or_condition_fails_closed(self):
        self.blocked(self.expr()+' || true')

    def test_10_extra_and_clause_fails_closed(self):
        self.blocked(self.expr()+' && true')

    def test_11_reversed_expression_not_whitelisted(self):
        self.blocked(f"resource.name.startsWith('{self.prefix}') && "
                     "resource.type == 'storage.googleapis.com/Object'")

    def test_12_ten_exact_targets_still_required(self):
        self.put(self.expr())
        self.doc['targets'].pop()
        with self.assertRaisesRegex(ValueError,'IAM_OBJETIVOS_EXACTOS_NO_COINCIDEN'):
            self.check()

    def test_13_unknown_deny_still_blocks(self):
        self.put(self.expr())
        self.doc['independent_deny_review']['outcome']='UNKNOWN'
        with self.assertRaisesRegex(ValueError,'IAM_DENY_REVIEW_NO_ACREDITADA'):
            self.check()

    def test_14_policy_duplicate_binding_blocks(self):
        self.put(self.expr())
        import copy
        binding=copy.deepcopy(self.doc['allow_policy']['bindings'][0])
        self.doc['allow_policy']['bindings'].append(binding)
        with self.assertRaisesRegex(ValueError,'IAM_POLICY_NO_ACREDITA_TODOS_LOS_OBJETOS'):
            self.check()

    def test_15_invalid_type_prefix_but_correct_name_blocks(self):
        self.blocked(self.expr(obj_type='storage.googleapis.com/ObjectX'))

    def test_16_other_function_not_allowed(self):
        self.blocked(self.expr().replace('startsWith','endsWith'))
