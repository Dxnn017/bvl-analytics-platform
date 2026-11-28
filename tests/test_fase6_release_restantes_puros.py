import sys
from pathlib import Path
import io
import unittest
import zipfile

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'/'processing'))
import fase6_release_restantes as rel

class TestRelease(unittest.TestCase):
    def test_reproducible_zip(self):
        a=rel.deterministic_zip({'b.py':b'print(2)\n','a.py':b'print(1)\n'})
        b=rel.deterministic_zip({'a.py':b'print(1)\n','b.py':b'print(2)\n'})
        self.assertEqual(a,b)
        with zipfile.ZipFile(io.BytesIO(a)) as z:
            self.assertEqual(z.namelist(),['a.py','b.py'])
    def test_module_count(self):
        self.assertEqual(len(rel.MODULES),19)
        self.assertEqual(len(set(rel.MODULES)),len(rel.MODULES))
        self.assertEqual(len(set(rel.MODULES.values())),len(rel.MODULES))
    def test_rejects_path_traversal(self):
        with self.assertRaises(ValueError):
            rel.deterministic_zip({'../bad.py':b'bad'})
    def test_rejects_non_python(self):
        with self.assertRaises(ValueError):
            rel.deterministic_zip({'bad.txt':b'bad'})
    def test_refuses_empty(self):
        with self.assertRaises(ValueError):
            rel.deterministic_zip({})

if __name__=='__main__':unittest.main()
