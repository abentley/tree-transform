from contextlib import contextmanager
import os
from shutil import rmtree
from tempfile import mkdtemp
from unittest import TestCase

from tree_transform.tree_transform import (
    FSTree,
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


class TreeTestMixin:

    def test_write_content(self):
        with self.tree() as tree:
            tree.write_content('foo', ['asdf'])
            self.assertEqual(''.join(tree.read_content('foo')), 'asdf')

    def test_rename(self):
        with self.tree() as tree:
            tree.write_content('foo', ['asdf'])
            tree.rename('foo', 'bar')
            self.assertEqual(''.join(tree.read_content('bar')), 'asdf')

    def test_mkdir(self):
        with self.tree() as tree:
            tree.mkdir('dir1')
            tree.write_content('dir1/foo', ['asdf'])

    def test_make_subtree(self):
        with self.tree() as tree:
            tree.mkdir('dir1')
            subtree = tree.make_subtree('dir1')
            subtree.write_content('foo', ['asdf'])
            self.assertEqual(''.join(tree.read_content('dir1/foo')), 'asdf')

    def test_make_temp_tree(self):
        with self.tree() as tree:
            temp_tree = tree.make_temp_tree()
            relpath = os.path.relpath(temp_tree.tree_root, tree.tree_root)
            self.assertNotIn('..', relpath)
            temp_tree.write_content('n-foo', ['asdf'])
            tree.rename(temp_tree._abspath('n-foo'), 'f-foo')
            self.assertEqual(''.join(tree.read_content('f-foo')), 'asdf')

    def test_mkdtemp(self):
        with self.tree() as tree:
            name = tree.mkdtemp()
            self.assertRegexpMatches(name, 'transform-')


class TestMemoryTree(TestCase, TreeTestMixin):

    @contextmanager
    def tree(self):
        yield MemoryTree()


class TestFSTree(TestCase, TreeTestMixin):

    @contextmanager
    def tree(self):
        with temp_dir() as tree_root:
            yield FSTree(tree_root)


class TestTreeTransform(TestCase):

    def test__tree_path_to_id(self):
        tt = TreeTransform(MemoryTree())
        self.assertEqual('e-.', tt._tree_path_to_id('.'))
        self.assertEqual('e-.', tt._tree_path_to_id('foo/..'))
        with self.assertRaisesRegexp(ValueError, 'Path outside tree.'):
            tt._tree_path_to_id('..')
        with self.assertRaisesRegexp(ValueError, 'Path outside tree.'):
            tt._tree_path_to_id('foo/../..')

    def test__tree_id_to_path(self):
        tt = TreeTransform(MemoryTree())
        self.assertEqual('hello', tt._tree_id_to_path('e-hello'))
        with self.assertRaisesRegexp(ValueError, 'Invalid path.'):
            tt._tree_id_to_path('ehello')
        with self.assertRaisesRegexp(ValueError, 'Invalid path.'):
            tt._tree_id_to_path('-hello')

    def test_get_existing_id(self):
        mem_tree = MemoryTree()
        tt = TreeTransform(mem_tree)
        self.assertEqual('e-file1', tt.acquire_existing_id('file1'))

    def test_get_name_info(self):
        mem_tree = MemoryTree()
        tt = TreeTransform(mem_tree)
        file1 = tt.acquire_existing_id('file1')
        parent, name = tt.get_name_info(file1)
        self.assertEqual(parent, tt._tree_path_to_id('.'))
        self.assertEqual(name, 'file1')

    def test_set_name_info(self):
        mem_tree = MemoryTree()
        tt = TreeTransform(mem_tree)
        file1 = tt.acquire_existing_id('file1')
        dir1 = tt._tree_path_to_id('dir1')
        tt.set_name_info(file1, dir1, 'file2')
        parent, name = tt.get_name_info(file1)
        self.assertEqual(parent, dir1)
        self.assertEqual(name, 'file2')

    def test_get_final_path(self):
        mem_tree = MemoryTree()
        tt = TreeTransform(mem_tree)
        file1 = tt.acquire_existing_id('file1')
        dir1 = tt._tree_path_to_id('dir1')
        self.assertEqual(tt.get_final_path(file1), 'file1')
        tt.set_name_info(file1, dir1, 'file2')
        self.assertEqual(tt.get_final_path(file1), 'dir1/file2')

    def test_apply(self):
        mem_tree = MemoryTree()
        mem_tree.write_content('file1', ['hello'])
        tt = TreeTransform(mem_tree)
        file1 = tt.acquire_existing_id('file1')
        dir1 = tt._tree_path_to_id('dir1')
        self.assertEqual(tt.get_final_path(file1), 'file1')
        tt.set_name_info(file1, dir1, 'file2')
        tt.apply()
        self.assertEqual('hello',
                         ''.join(mem_tree.read_content('dir1/file2')))
