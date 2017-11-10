from contextlib import contextmanager
import os
from shutil import rmtree
from tempfile import mkdtemp
from unittest import TestCase

from tree_transform.tree_transform import (
    FSTree,
    InactiveTransform,
    IsDirectory,
    MemoryTree,
    NotPending,
    NoParent,
    NoSuchFile,
    ParentNotDir,
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

    def test_read_content_no_file(self):
        with self.tree() as tree:
            with self.assertRaises(NoSuchFile):
                tree.read_content('foo')

    def test_read_content_directory(self):
        with self.tree() as tree:
            tree.mkdir('foo')
            with self.assertRaises(IsDirectory):
                tree.read_content('foo')

    def test_write_content(self):
        with self.tree() as tree:
            tree.write_content('foo', ['asdf'])
            self.assertEqual(''.join(tree.read_content('foo')), 'asdf')

    def test_write_content_no_parent(self):
        with self.tree() as tree:
            with self.assertRaises(NoParent):
                tree.write_content('non-existent/foo', ['asdf'])

    def test_write_content_parent_not_dir(self):
        with self.tree() as tree:
            with self.assertRaises(ParentNotDir):
                tree.write_content('file', ['asdf'])
                tree.write_content('file/foo', ['asdf'])

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
        with self.assertRaisesRegexp(ValueError, 'Invalid id.'):
            tt._tree_id_to_path('ehello')
        with self.assertRaisesRegexp(ValueError, 'Invalid id.'):
            tt._tree_id_to_path('-hello')

    def test_get_existing_id(self):
        mem_tree = MemoryTree()
        tt = TreeTransform(mem_tree, write=False)
        with tt:
            self.assertEqual('e-file1', tt.acquire_existing_id('file1'))

    def test_get_parent(self):
        mem_tree = MemoryTree()
        tt = TreeTransform(mem_tree, write=False)
        with self.assertRaises(NotPending):
            parent = tt.get_parent('e-file1')
        with tt:
            file1 = tt.acquire_existing_id('file1')
            parent = tt.get_parent(file1)
            self.assertEqual(parent, tt._tree_path_to_id('.'))
            root = tt.acquire_existing_id('.')
            with self.assertRaises(KeyError):
                parent = tt.get_parent(root)

    def test_get_name(self):
        mem_tree = MemoryTree()
        tt = TreeTransform(mem_tree, write=False)
        with self.assertRaises(NotPending):
            tt.get_name('file1')
        with tt:
            file1 = tt.acquire_existing_id('file1')
            name = tt.get_name(file1)
            self.assertEqual(name, 'file1')
            root = tt.acquire_existing_id('.')
            with self.assertRaises(KeyError):
                tt.get_name(root)

    def test_set_name_info(self):
        mem_tree = MemoryTree()
        tt = TreeTransform(mem_tree, write=False)
        with self.assertRaises(NotPending):
            tt.set_name_info('foo', 'bar', 'file2')
        with tt:
            file1 = tt.acquire_existing_id('file1')
            dir1 = tt._tree_path_to_id('dir1')
            tt.set_name_info(file1, dir1, 'file2')
            self.assertEqual(tt.get_parent(file1), dir1)
            self.assertEqual(tt.get_name(file1), 'file2')

    def test_get_final_path(self):
        mem_tree = MemoryTree()
        tt = TreeTransform(mem_tree, write=False)
        with self.assertRaises(NotPending):
            tt.get_final_path('file1')
        with tt:
            file1 = tt.acquire_existing_id('file1')
            dir1 = tt._tree_path_to_id('dir1')
            self.assertEqual(tt.get_final_path(file1), 'file1')
            tt.set_name_info(file1, dir1, 'file2')
            self.assertEqual(tt.get_final_path(file1), 'dir1/file2')

    def test__apply(self):
        mem_tree = MemoryTree()
        mem_tree.write_content('file1', ['hello'])
        tt = TreeTransform(mem_tree, write=False)
        with self.assertRaises(NotPending):
            tt._apply()
        with tt:
            file1 = tt.acquire_existing_id('file1')
            dir1 = tt._tree_path_to_id('dir1')
            self.assertEqual(tt.get_final_path(file1), 'file1')
            tt.set_name_info(file1, dir1, 'file2')
            tt._apply()
            self.assertEqual('hello',
                             ''.join(mem_tree.read_content('dir1/file2')))

    def test_generate_renames(self):
        mem_tree = MemoryTree()
        tt = TreeTransform(mem_tree, write=False)
        with tt:
            file1 = tt.acquire_existing_id('file1')
            dir1 = tt._tree_path_to_id('dir1')
            tt.set_name_info(file1, dir1, 'file2')
            self.assertEqual([('file1', 'dir1/file2')],
                              list(tt.generate_renames()))

    def test_with(self):
        mem_tree = MemoryTree()
        mem_tree.write_content('file1', ['hello'])
        tt = TreeTransform(mem_tree)
        self.assertIs(InactiveTransform, type(tt._name_info))
        with tt:
            self.assertEqual({}, tt._name_info)
            file1 = tt.acquire_existing_id('file1')
            dir1 = tt._tree_path_to_id('dir1')
            self.assertEqual(tt.get_final_path(file1), 'file1')
            tt.set_name_info(file1, dir1, 'file2')
        self.assertIs(InactiveTransform, type(tt._name_info))
        self.assertEqual('hello',
                         ''.join(mem_tree.read_content('dir1/file2')))

    def test_with_exception(self):
        mem_tree = MemoryTree()
        mem_tree.write_content('file1', ['hello'])
        class SentryException(Exception):
            pass
        with self.assertRaises(SentryException):
            with TreeTransform(mem_tree) as tt:
                file1 = tt.acquire_existing_id('file1')
                dir1 = tt._tree_path_to_id('dir1')
                tt.set_name_info(file1, dir1, 'file2')
                raise SentryException
        with self.assertRaises(NoSuchFile):
            mem_tree.read_content('dir1/file2')
