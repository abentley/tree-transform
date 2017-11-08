from contextlib import contextmanager
import os
from shutil import rmtree
from tempfile import mkdtemp
from unittest import TestCase

from tree_transform.tree_transform import (
    MemoryTree,
    TreeTransform,
    )


@contextmanager
def temp_dir():
    temp = mkdtemp()
    try:
        yield temp
    finally:
        rmtree(temp)


class TestMemoryTree(TestCase):

    def test_path_to_id(self):
        mem_tree = MemoryTree()
        self.assertEqual('e-.', mem_tree.path_to_id('.'))
        self.assertEqual('e-.', mem_tree.path_to_id('foo/..'))
        with self.assertRaisesRegexp(ValueError, 'Path outside tree.'):
            mem_tree.path_to_id('..')
        with self.assertRaisesRegexp(ValueError, 'Path outside tree.'):
            mem_tree.path_to_id('foo/../..')

    def test_write_content(self):
        mem_tree = MemoryTree()
        mem_tree.write_content('foo', ['asdf'])
        self.assertEqual(mem_tree.read_content('foo'), 'asdf')

    def test_rename(self):
        mem_tree = MemoryTree()
        mem_tree.write_content('foo', ['asdf'])
        mem_tree.rename('foo', 'bar')
        self.assertEqual(mem_tree.read_content('bar'), 'asdf')


class TestTreeTransform(TestCase):

    def test_get_existing_id(self):
        mem_tree = MemoryTree()
        tt = TreeTransform(mem_tree)
        self.assertEqual('e-file1', tt.acquire_existing_id('file1'))

    def test_get_name_info(self):
        mem_tree = MemoryTree()
        tt = TreeTransform(mem_tree)
        file1 = tt.acquire_existing_id('file1')
        parent, name = tt.get_name_info(file1)
        self.assertEqual(parent, mem_tree.path_to_id('.'))
        self.assertEqual(name, 'file1')

    def test_set_name_info(self):
        mem_tree = MemoryTree()
        tt = TreeTransform(mem_tree)
        file1 = tt.acquire_existing_id('file1')
        dir1 = mem_tree.path_to_id('dir1')
        tt.set_name_info(file1, dir1, 'file2')
        parent, name = tt.get_name_info(file1)
        self.assertEqual(parent, dir1)
        self.assertEqual(name, 'file2')

    def test_get_final_path(self):
        mem_tree = MemoryTree()
        tt = TreeTransform(mem_tree)
        file1 = tt.acquire_existing_id('file1')
        dir1 = mem_tree.path_to_id('dir1')
        self.assertEqual(tt.get_final_path(file1), 'file1')
        tt.set_name_info(file1, dir1, 'file2')
        self.assertEqual(tt.get_final_path(file1), 'dir1/file2')

    def test_apply(self):
        mem_tree = MemoryTree()
        mem_tree.write_content('file1', ['hello'])
        tt = TreeTransform(mem_tree)
        file1 = tt.acquire_existing_id('file1')
        dir1 = mem_tree.path_to_id('dir1')
        self.assertEqual(tt.get_final_path(file1), 'file1')
        tt.set_name_info(file1, dir1, 'file2')
        tt.apply()
        self.assertEqual('hello', mem_tree.read_content('dir1/file2'))
