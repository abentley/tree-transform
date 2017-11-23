from contextlib import contextmanager
import os
from shutil import rmtree
from tempfile import mkdtemp
from unittest import TestCase

from tree_transform.tree_transform import (
    FSTree,
    InactiveTransform,
    IsDirectory,
    StoreTree,
    NotPending,
    NoParent,
    NoSuchFile,
    OverlayFileStore,
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


class ReadOnlyTreeTestMixin:

    def assertCountEqual(self, *args, **kwargs):
        return self.assertItemsEqual(*args, **kwargs)

    def test_read_content_no_file(self):
        with self.setup_tree() as tree:
            with self.assertRaises(NoSuchFile):
                self.actual_tree(tree).read_content('foo')

    def test_read_content_directory(self):
        with self.setup_tree() as tree:
            tree.mkdir('foo')
            with self.assertRaises(IsDirectory):
                self.actual_tree(tree).read_content('foo')

    def test_make_subtree(self):
        with self.setup_tree() as tree:
            tree.mkdir('dir1')
            actual = self.actual_tree(tree)
            subtree = actual.make_subtree('dir1')
            tree.write_content('dir1/foo', ['asdf'])
            self.assertEqual(''.join(subtree.read_content('foo')), 'asdf')

    def test_make_readonly_version(self):
        with self.setup_tree() as tree:
            tree.mkdir('dir1')
            actual = self.actual_tree(tree)
            readonly = actual.readonly_version()
            tree.write_content('dir1/foo', ['asdf'])
            self.assertEqual(''.join(readonly.read_content('dir1/foo')),
                             'asdf')


class TreeTestMixin(ReadOnlyTreeTestMixin):

    def test_write_content(self):
        with self.setup_tree() as tree:
            tree.write_content('foo', ['asdf'])
            self.assertEqual(''.join(tree.read_content('foo')), 'asdf')

    def test_write_content_no_parent(self):
        with self.setup_tree() as tree:
            with self.assertRaises(NoParent):
                actual = self.actual_tree(tree)
                actual.write_content('non-existent/foo', ['asdf'])

    def test_write_content_parent_not_dir(self):
        with self.setup_tree() as tree:
            tree.write_content('file', ['asdf'])
            actual = self.actual_tree(tree)
            with self.assertRaises(ParentNotDir):
                actual.write_content('file/foo', ['asdf'])

    def test_rename(self):
        with self.setup_tree() as tree:
            tree.write_content('foo', ['asdf'])
            actual = self.actual_tree(tree)
            actual.rename('foo', 'bar')
            self.assertEqual(''.join(actual.read_content('bar')), 'asdf')

    def test_rename_dir(self):
        with self.setup_tree() as tree:
            tree.write_content('foo', ['asdf'])
            tree.mkdir('bar')
            actual = self.actual_tree(tree)
            actual.rename('foo', 'bar/foo')
            self.assertEqual(''.join(actual.read_content('bar/foo')), 'asdf')

    def test_rename_dir_missing(self):
        with self.setup_tree() as tree:
            tree.write_content('foo', ['asdf'])
            actual = self.actual_tree(tree)
            with self.assertRaises(NoParent):
                actual.rename('foo', 'bar/foo')

    def test_rename_parent_not_dir(self):
        with self.setup_tree() as tree:
            tree.write_content('foo', ['asdf'])
            tree.write_content('bar', ['asdf'])
            actual = self.actual_tree(tree)
            with self.assertRaises(ParentNotDir):
                actual.rename('foo', 'bar/foo')

    def test_mkdir(self):
        with self.setup_tree() as tree:
            actual = self.actual_tree(tree)
            actual.mkdir('dir1')
            actual.write_content('dir1/foo', ['asdf'])

    def test_rmtree(self):
        with self.setup_tree() as tree:
            tree.mkdir('dir1')
            tree.write_content('dir1/foo', ['asdf'])
            actual = self.actual_tree(tree)
            actual.rmtree('dir1')
            with self.assertRaises(NoSuchFile):
                actual.read_content('dir1')
            with self.assertRaises(NoSuchFile):
                actual.read_content('dir1/foo')

    def test_iter_subppaths(self):
        with self.setup_tree() as setup:
            actual = self.actual_tree(setup)
            self.assertCountEqual([], actual.iter_subpaths('dir1'))
            setup.mkdir('dir1')
            self.assertCountEqual(['dir1'], actual.iter_subpaths('dir1'))
            setup.mkdir('dir1/dir2')
            self.assertCountEqual(['dir1', 'dir1/dir2'],
                                  actual.iter_subpaths('dir1'))
            setup.write_content('dir1/file1', ['hello'])
            self.assertCountEqual(['dir1', 'dir1/dir2', 'dir1/file1'],
                                  actual.iter_subpaths('dir1'))

    def test_ignore_non_parent(self):
        with self.setup_tree() as setup:
            actual = self.actual_tree(setup)
            setup.mkdir('dir1')
            self.assertCountEqual([], actual.iter_subpaths('dir'))

    def test_apply_renames(self):
        with self.setup_tree() as tree:
            tree.write_content('file1', ['hello'])
            tree.mkdir('dir1')
            actual = self.actual_tree(tree)
            tt = TreeTransform(actual, write=False)
            with tt:
                file1 = tt.acquire_existing_id('file1')
                dir1 = tt._tree_path_to_id('dir1')
                self.assertEqual(tt.get_final_path(file1), 'file1')
                tt.set_name_info(file1, dir1, 'file2')
                actual.apply_renames(tt.generate_renames())
                self.assertEqual('hello',
                                 ''.join(actual.read_content('dir1/file2')))

    def test_make_writable_subtree(self):
        with self.setup_tree() as tree:
            tree.mkdir('dir1')
            actual = self.actual_tree(tree)
            subtree = actual.make_subtree('dir1')
            subtree.write_content('foo', ['asdf'])
            self.assertEqual(''.join(actual.read_content('dir1/foo')), 'asdf')

    def test_make_temp_tree(self):
        with self.setup_tree() as tree:
            actual = self.actual_tree(tree)
            temp_tree = actual.make_temp_tree()
            relpath = os.path.relpath(temp_tree.tree_root, actual.tree_root)
            self.assertNotIn('..', relpath)
            temp_tree.write_content('n-foo', ['asdf'])
            actual.rename(temp_tree.full_path('n-foo'), 'f-foo')
            self.assertEqual(''.join(actual.read_content('f-foo')), 'asdf')

    def test_mkdtemp(self):
        with self.setup_tree() as tree:
            actual = self.actual_tree(tree)
            name = actual.mkdtemp()
            self.assertRegexpMatches(name, 'transform-')


class TestReadOnlyStoreTree(TestCase, ReadOnlyTreeTestMixin):

    @contextmanager
    def setup_tree(self):
        yield StoreTree()

    def actual_tree(self, tree):
        return tree.readonly_version()


class TestStoreTree(TestCase, TreeTestMixin):

    @contextmanager
    def setup_tree(self):
        yield StoreTree()

    def actual_tree(self, tree):
        return tree


class TestOverlayTree(TestCase, TreeTestMixin):

    @contextmanager
    def setup_tree(self):
        yield StoreTree()

    def actual_tree(self, tree):
        return StoreTree(file_store=OverlayFileStore(tree.readonly_version()))


class TestOverlayOnlyTree(TestCase, TreeTestMixin):

    @contextmanager
    def setup_tree(self):
        base = StoreTree().readonly_version()
        yield StoreTree(file_store=OverlayFileStore(base))

    def actual_tree(self, tree):
        return tree


class TestFSTree(TestCase, TreeTestMixin):

    @contextmanager
    def setup_tree(self):
        with temp_dir() as tree_root:
            yield FSTree(tree_root)

    def actual_tree(self, tree):
        return tree


class TestTreeTransform(TestCase):

    def test__tree_path_to_id(self):
        tt = TreeTransform(StoreTree())
        self.assertEqual('e-.', tt._tree_path_to_id('.'))
        self.assertEqual('e-.', tt._tree_path_to_id('foo/..'))
        with self.assertRaisesRegexp(ValueError, 'Path outside tree.'):
            tt._tree_path_to_id('..')
        with self.assertRaisesRegexp(ValueError, 'Path outside tree.'):
            tt._tree_path_to_id('foo/../..')

    def test__tree_id_to_path(self):
        tt = TreeTransform(StoreTree())
        self.assertEqual('hello', tt._tree_id_to_path('e-hello'))
        with self.assertRaisesRegexp(ValueError, 'Invalid id.'):
            tt._tree_id_to_path('ehello')
        with self.assertRaisesRegexp(ValueError, 'Invalid id.'):
            tt._tree_id_to_path('-hello')

    def test_get_existing_id(self):
        store_tree = StoreTree()
        tt = TreeTransform(store_tree, write=False)
        with tt:
            self.assertEqual('e-file1', tt.acquire_existing_id('file1'))

    def test_make_new_id(self):
        tt = TreeTransform(StoreTree(), write=False)
        with self.assertRaises(NotPending):
            tt.make_new_id('foo')
        with tt:
            self.assertEqual(tt.make_new_id('foo'), 'n-0-foo')
            self.assertEqual(tt.make_new_id('foo'), 'n-1-foo')

    def test_get_parent(self):
        store_tree = StoreTree()
        tt = TreeTransform(store_tree, write=False)
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
        store_tree = StoreTree()
        tt = TreeTransform(store_tree, write=False)
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
        store_tree = StoreTree()
        tt = TreeTransform(store_tree, write=False)
        with self.assertRaises(NotPending):
            tt.set_name_info('foo', 'bar', 'file2')
        with tt:
            file1 = tt.acquire_existing_id('file1')
            dir1 = tt._tree_path_to_id('dir1')
            tt.set_name_info(file1, dir1, 'file2')
            self.assertEqual(tt.get_parent(file1), dir1)
            self.assertEqual(tt.get_name(file1), 'file2')

    def test_get_final_path(self):
        store_tree = StoreTree()
        tt = TreeTransform(store_tree, write=False)
        with self.assertRaises(NotPending):
            tt.get_final_path('file1')
        with tt:
            file1 = tt.acquire_existing_id('file1')
            dir1 = tt._tree_path_to_id('dir1')
            self.assertEqual(tt.get_final_path(file1), 'file1')
            tt.set_name_info(file1, dir1, 'file2')
            self.assertEqual(tt.get_final_path(file1), 'dir1/file2')

    def test_generate_renames(self):
        store_tree = StoreTree()
        tt = TreeTransform(store_tree, write=False)
        with tt:
            file1 = tt.acquire_existing_id('file1')
            dir1 = tt._tree_path_to_id('dir1')
            file1_path = tt._new_contents.full_path(file1)
            tt.set_name_info(file1, dir1, 'file2')
            self.assertEqual(
                [('file1', file1_path),
                 (file1_path, 'dir1/file2')],
                tt.generate_renames())

    def test_generate_renames_dir_swap(self):
        store_tree = StoreTree()
        tt = TreeTransform(store_tree, write=False)
        with tt:
            dir1 = tt._tree_path_to_id('dir1')
            dir2 = tt._tree_path_to_id('dir1/dir2')
            root = tt._tree_path_to_id('.')
            tt.set_name_info(dir1, dir2, 'dir1')
            tt.set_name_info(dir2, root, 'dir2')
            dir1_path = tt._new_contents.full_path(dir1)
            dir2_path = tt._new_contents.full_path(dir2)
            self.assertEqual(
                [('dir1/dir2', dir2_path),
                 ('dir1', dir1_path),
                 (dir2_path, 'dir2'),
                 (dir1_path, 'dir2/dir1')],
                tt.generate_renames())

    def test_with(self):
        store_tree = StoreTree()
        store_tree.write_content('file1', ['hello'])
        store_tree.mkdir('dir1')
        tt = TreeTransform(store_tree)
        self.assertIs(InactiveTransform, type(tt._name_info))
        self.assertIs(InactiveTransform, type(tt.id_counter))
        self.assertIs(None, tt._new_contents)
        with tt:
            self.assertEqual({}, tt._name_info)
            file1 = tt.acquire_existing_id('file1')
            dir1 = tt._tree_path_to_id('dir1')
            self.assertEqual(tt.get_final_path(file1), 'file1')
            tt.set_name_info(file1, dir1, 'file2')
        self.assertIs(InactiveTransform, type(tt._name_info))
        self.assertIs(InactiveTransform, type(tt.id_counter))
        self.assertEqual('hello',
                         ''.join(store_tree.read_content('dir1/file2')))

    def test_subtrees(self):
        store_tree = StoreTree()
        tt = TreeTransform(store_tree)
        self.assertIs(None, tt._temp_tree)
        with tt:
            relative_root = os.path.relpath(tt._temp_tree.tree_root,
                                            store_tree.tree_root)
            self.assertNotIn('..', relative_root)
            tt._temp_tree.write_content('file1', ['hello'])
            full_path = os.path.join(relative_root, 'file1')
            self.assertEqual('hello',
                             ''.join(store_tree.read_content(full_path)))
        self.assertIs(None, tt._temp_tree)
        with self.assertRaises(NoSuchFile):
            store_tree.read_content(full_path)

    def test_with_exception(self):
        store_tree = StoreTree()
        store_tree.write_content('file1', ['hello'])

        class SentryException(Exception):
            pass

        with self.assertRaises(SentryException):
            with TreeTransform(store_tree) as tt:
                file1 = tt.acquire_existing_id('file1')
                dir1 = tt._tree_path_to_id('dir1')
                tt.set_name_info(file1, dir1, 'file2')
                raise SentryException
        with self.assertRaises(NoSuchFile):
            store_tree.read_content('dir1/file2')

    def test_create_file(self):
        store_tree = StoreTree()
        tt = TreeTransform(store_tree)
        with self.assertRaises(NotPending):
            tt.create_file('name1', 'parent', ['hello'])
        with tt:
            parent_id = tt.acquire_existing_id('.')
            file_id = tt.create_file('name1', parent_id, ['hello'])
            source = tt._new_contents.full_path(file_id)
            target = tt.get_final_path(file_id)
            self.assertEqual(tt.generate_renames(), [(source, target)])
        self.assertEqual('hello', ''.join(store_tree.read_content('name1')))

    def test_delete(self):
        store_tree = StoreTree()
        store_tree.write_content('foo', ['hello'])
        store_tree.mkdir('bar')
        tt = TreeTransform(store_tree)
        with self.assertRaises(NotPending):
            tt.delete('e-foo')
        with tt:
            tt.delete(tt.acquire_existing_id('foo'))
            tt.delete(tt.acquire_existing_id('bar'))
            store_tree.read_content('foo')
            with self.assertRaises(IsDirectory):
                store_tree.read_content('bar')
        with self.assertRaises(NotPending):
            tt.delete('e-foo')
        with self.assertRaises(NoSuchFile):
            store_tree.read_content('foo')
        with self.assertRaises(NoSuchFile):
            store_tree.read_content('bar')
