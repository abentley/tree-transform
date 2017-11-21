import errno
from itertools import count
import os
import random
from shutil import rmtree
from tempfile import mkdtemp

__metaclass__ = type


class NoSuchFile(Exception):
    """Raised when no such file exists."""


class NoParent(Exception):
    """Raised when attempting to use a path whose parent dir doesn't exist."""


class ParentNotDir(Exception):
    """Raised when attempting to treat a file as a directory."""


class IsDirectory(Exception):
    """Raised when a directory is treated like a regular file."""


class BaseTree:

    def __init__(self, tree_root):
        self.tree_root = tree_root

    def apply_renames(self, renames):
        for old_path, new_path in renames:
            self.rename(old_path, new_path)

    def full_path(self, path):
        return os.path.join(self.tree_root, path)

    def make_temp_tree(self):
        tree_root = self.mkdtemp()
        return self.make_subtree(tree_root)


class FSTree(BaseTree):
    """Represents a filesystem tree."""

    def make_subtree(self, path):
        return type(self)(self.full_path(path))

    def write_content(self, path, strings):
        """Store content from iterable of strings."""
        try:
            f = open(self.full_path(path), 'w')
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise NoParent
            if e.errno == errno.ENOTDIR:
                raise ParentNotDir
            else:
                raise
        with f:
            f.writelines(strings)

    def mkdir(self, path):
        os.mkdir(self.full_path(path))

    def mkdtemp(self):
        return mkdtemp(dir=self.tree_root, prefix='transform-')

    def rmtree(self, path):
        rmtree(self.full_path(path))

    def read_content(self, path):
        """Store content from iterable of strings."""
        try:
            f = open(os.path.join(self.tree_root, path), 'r')
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise NoSuchFile
            elif e.errno == errno.EISDIR:
                raise IsDirectory
            else:
                raise
        with f:
            return f.readlines()

    def rename(self, old_path, new_path):
        old_path = self.full_path(old_path)
        new_path = self.full_path(new_path)
        try:
            os.rename(old_path, new_path)
        except OSError as e:
            if e.errno == errno.ENOENT:
                raise NoParent
            elif e.errno == errno.ENOTDIR:
                raise ParentNotDir
            else:
                raise


class MemoryFileStore:
    """Represents a key/value file store (blob store) in memory.

    This does not enforce filesystem restrictions like the idea that every file
    must have a parent directory.
    """

    DIRECTORY = object()

    def __init__(self, content):
        self._content = content

    def require_parent(self, full_path):
        parent = os.path.dirname(full_path)
        parent_content = self._content.get(parent)
        if parent_content is None:
            raise NoParent
        if parent_content is not self.DIRECTORY:
            raise ParentNotDir

    def write_content(self, full_path, strings):
        """Store content from iterable of strings."""
        self._content[full_path] = ''.join(strings)

    def mkdir(self, full_path):
        self._content[full_path] = self.DIRECTORY

    def read_content(self, full_path):
        """Access content as iterable of strings."""
        try:
            content = self._content[full_path]
        except KeyError:
            raise NoSuchFile
        if content is self.DIRECTORY:
            raise IsDirectory
        return iter([content])

    def rmtree(self, full_path):
        for key in list(self._content.keys()):
            if key == full_path or key.startswith(full_path + os.sep):
                del self._content[key]

    def rename(self, old_path, new_path):
        self._content[new_path] = self._content.pop(old_path)


class MemoryTree(BaseTree):
    """Represents a filesystem tree in memory."""

    def __init__(self, tree_root='', file_store=None):
        super(MemoryTree, self).__init__(tree_root)
        if file_store is None:
            content = {tree_root: MemoryFileStore.DIRECTORY}
            file_store = MemoryFileStore(content)
        self._file_store = file_store

    def make_subtree(self, path):
        return type(self)(self.full_path(path), self._file_store)

    def write_content(self, path, strings):
        """Store content from iterable of strings."""
        full_path = self.full_path(path)
        self._file_store.require_parent(full_path)
        return self._file_store.write_content(full_path, strings)

    def mkdir(self, path):
        return self._file_store.mkdir(self.full_path(path))

    def rmtree(self, path):
        return self._file_store.rmtree(path)

    def mkdtemp(self):
        name = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz')
                       for x in range(8))
        name = 'transform-' + name
        self.mkdir(name)
        return name

    def read_content(self, path):
        """Access content as iterable of strings."""
        return self._file_store.read_content(self.full_path(path))

    def rename(self, old_path, new_path):
        full_new_path = self.full_path(new_path)
        self._file_store.require_parent(full_new_path)
        self._file_store.rename(self.full_path(old_path), full_new_path)


class NotPending(Exception):
    """Raised when attempting to access the transform while inactive."""


class InactiveTransform:
    """Used for member variables when the transform is inactive.

    This way, we pay the cost of defending against developer error only when
    the error is happening.  Otherwise, we do not need extra checks to see
    whether the transform is active.
    """

    def __contains__(self, x):
        raise NotPending

    def __setitem__(self, key, value):
        raise NotPending

    def __getitem__(self, key):
        raise NotPending

    def get(self, key):
        raise NotPending

    def items(self):
        raise NotPending

    def next(self):
        raise NotPending

    def __next__(self):
        raise NotPending

    def add(self, key):
        raise NotPending


class TreeTransform:
    """Apply FS tree changes atomically.

    This is a context manager that allows changes to be applied to a tree.

    By default, the changes are applied on successful exit.

    Basically, filesytem operations are applied as normal, but to temporary
    copies of files.  On exit, the temporary copies are renamed into place.
    """

    def __init__(self, tree, write=True):
        self.tree = tree
        self.write = write
        self.id_counter = count()
        self._mark_inactive()

    def _mark_inactive(self):
        self._name_info = InactiveTransform()
        self._temp_tree = None
        self._new_contents = None
        self._new_contents_path = InactiveTransform()
        self.id_counter = InactiveTransform()
        self._remove_ids = InactiveTransform()

    def __enter__(self):
        self._name_info = {}
        self._temp_tree = self.tree.make_temp_tree()
        self._temp_tree.mkdir('new')
        self._new_contents = self._temp_tree.make_subtree('new')
        self._new_contents_path = {}
        self._temp_tree.mkdir('old')
        self._old_contents = self._temp_tree.make_subtree('old')
        self._remove_ids = set()
        self.id_counter = count()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if (exc_type, exc_value, exc_traceback) == (None, None, None):
            if self.write:
                self.tree.apply_renames(self.generate_renames())
        self.tree.rmtree(self._temp_tree.tree_root)
        self._mark_inactive()

    def _tree_path_to_id(self, path):
        normpath = os.path.normpath(path)
        if normpath.startswith('..'):
            raise ValueError('Path outside tree.')
        return 'e-{}'.format(normpath)

    def _tree_id_to_path(self, file_id):
        if file_id[:2] != 'e-':
            raise ValueError('Invalid id.')
        return file_id[2:]

    def make_new_id(self, name):
        return 'n-{}-{}'.format(next(self.id_counter), name)

    def acquire_existing_id(self, path):
        file_id = self._tree_path_to_id(path)
        if path in {'.', ''} or file_id in self._name_info:
            return file_id
        if file_id not in self._name_info:
            parent, name = os.path.split(path)
            parent_id = self.acquire_existing_id(parent)
            self.set_name_info(file_id, parent_id, name)
        return file_id

    def get_name(self, file_id):
        info = self._name_info.get(file_id)
        if info is not None:
            return info[1]
        path = self._tree_id_to_path(file_id)
        if path == '.':
            raise KeyError('.')
        return os.path.basename(path)

    def get_parent(self, file_id):
        info = self._name_info.get(file_id)
        if info is not None:
            return info[0]
        path = self._tree_id_to_path(file_id)
        if path == '.':
            raise KeyError('.')
        parent = os.path.dirname(path)
        return self._tree_path_to_id(parent)

    def set_name_info(self, file_id, parent_id, name):
        self._name_info[file_id] = (parent_id, name)

    def get_final_path(self, file_id, parent_id=None, name=None):
        if None in {parent_id, name}:
            try:
                parent_id, name = self._name_info[file_id]
            except KeyError:
                return self._tree_id_to_path(file_id)
        if parent_id == 'e-.':
            return name
        parent_path = self.get_final_path(parent_id)
        return os.path.join(parent_path, name)

    def create_file(self, name, parent_id, contents):
        file_id = self.make_new_id(name)
        self.set_name_info(file_id, parent_id, name)
        self._new_contents.write_content(file_id, contents)
        full_path = self._new_contents.full_path(file_id)
        self._new_contents_path[file_id] = full_path
        return file_id

    def delete(self, file_id):
        """Schedule a path to be deleted.

        The file will be moved to the old_contents subtree on apply, and then
        deleted with the rest of the temp dir on __exit__.
        """
        self._remove_ids.add(file_id)

    def _generate_remove_renames(self):
        remove_renames = []
        new_contents_path = dict(self._new_contents_path)
        relative_new_contents = os.path.relpath(self._new_contents.tree_root,
                                                self.tree.tree_root)
        relative_old_contents = os.path.relpath(self._old_contents.tree_root,
                                                self.tree.tree_root)
        for file_id, (parent_id, name) in self._name_info.items():
            if file_id in self._new_contents_path:
                continue
            if file_id in self._remove_ids:
                continue
            old_path = self._tree_id_to_path(file_id)
            new_path = os.path.join(relative_new_contents, file_id)
            remove_renames.append((old_path, new_path))
            new_contents_path[file_id] = new_path
        for file_id in self._remove_ids:
            old_path = self._tree_id_to_path(file_id)
            new_path = os.path.join(relative_old_contents, file_id)
            remove_renames.append((old_path, new_path))
        # Always remove children before parents
        remove_renames.sort(key=lambda p: p[0], reverse=True)
        return remove_renames, new_contents_path

    def _generate_insert_renames(self, new_contents_path):
        insert_renames = []
        for file_id, (parent_id, name) in self._name_info.items():
            old_path = new_contents_path.get(file_id)
            if old_path is None:
                continue
            new_path = self.get_final_path(file_id, parent_id, name)
            insert_renames.append((old_path, new_path))
        insert_renames.sort(key=lambda p: p[1])
        return insert_renames

    def generate_renames(self):
        """Generate renames for updating tree.

        Removals are always in child-to-parent order, because removing
        something before its children generally fails.

        Similarly, insertions are always in parent-to-child order, because
        creating something before its parent generally fails.

        Actual renames are decomposed into a removal and an insertion.  This
        handles certain corner cases nicely, e.g. if the parent and child swap
        places.
        """
        remove_renames, new_contents_path = self._generate_remove_renames()
        insert_renames = self._generate_insert_renames(new_contents_path)
        return remove_renames + insert_renames
