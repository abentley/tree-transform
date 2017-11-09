import os
import random
from tempfile import mkdtemp

__metaclass__ = type


class BaseTree:

    def __init__(self, tree_root):
        self.tree_root = tree_root

    def apply_renames(self, renames):
        for old_path, new_path in renames:
            self.rename(old_path, new_path)

    def _abspath(self, path):
        return os.path.join(self.tree_root, path)

    def make_temp_tree(self):
        tree_root = self.mkdtemp()
        return self.make_subtree(tree_root)


class FSTree(BaseTree):
    """Represents a filesystem tree."""

    def make_subtree(self, path):
        return type(self)(self._abspath(path))

    def write_content(self, path, strings):
        """Store content from iterable of strings."""
        with open(self._abspath(path), 'w') as f:
            f.writelines(strings)

    def mkdir(self, path):
        os.mkdir(self._abspath(path))

    def mkdtemp(self):
        return mkdtemp(dir=self.tree_root, prefix='transform-')

    def read_content(self, path):
        """Store content from iterable of strings."""
        with open(os.path.join(self.tree_root, path), 'r') as f:
            return f.readlines()

    def rename(self, old_path, new_path):
        old_path = self._abspath(old_path)
        new_path = self._abspath(new_path)
        os.rename(old_path, new_path)


class MemoryTree(BaseTree):
    """Represents a filesystem tree in memory."""

    DIRECTORY = object()

    def __init__(self, tree_root='/', content=None):
        super(MemoryTree, self).__init__(tree_root)
        if content is None:
            content = {}
        self._content = content

    def make_subtree(self, path):
        return type(self)(self._abspath(path), self._content)

    def write_content(self, path, strings):
        """Store content from iterable of strings."""
        self._content[self._abspath(path)] = ''.join(strings)

    def mkdir(self, path):
        self._content[self._abspath(path)] = self.DIRECTORY

    def mkdtemp(self):
        name = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz')
                       for x in range(8))
        return 'transform-' + name

    def read_content(self, path):
        """Access content as iterable of strings."""
        return iter([self._content[self._abspath(path)]])

    def rename(self, old_path, new_path):
        self._content[self._abspath(new_path)] = self._content.pop(
                self._abspath(old_path))


class TreeTransform:

    def __init__(self, tree):
        self.tree = tree
        self._name_info = {}

    def _tree_path_to_id(self, path):
        normpath = os.path.normpath(path)
        if normpath.startswith('..'):
            raise ValueError('Path outside tree.')
        return 'e-{}'.format(normpath)

    def _tree_id_to_path(self, file_id):
        if file_id[:2] != 'e-':
            raise ValueError('Invalid path.')
        return file_id[2:]

    def acquire_existing_id(self, path):
        file_id = self._tree_path_to_id(path)
        if path in {'.', ''} or file_id in self._name_info:
            return file_id
        if file_id not in self._name_info:
            parent, name = os.path.split(path)
            parent_id = self.acquire_existing_id(parent)
            self.set_name_info(file_id, parent_id, name)
        return file_id

    def get_name_info(self, file_id):
        return self._name_info[file_id]

    def set_name_info(self, file_id, parent_id, name):
        self._name_info[file_id] = (parent_id, name)

    def get_final_path(self, file_id):
        try:
            parent_id, name = self._name_info[file_id]
        except:
            return self._tree_id_to_path(file_id)
        if parent_id == 'e-.':
            return name
        parent_path = self.get_final_path(parent_id)
        return os.path.join(parent_path, name)

    def generate_renames(self):
        for file_id, (parent_id, name) in self._name_info.items():
            old_path = self._tree_id_to_path(file_id)
            new_path = self.get_final_path(file_id)
            yield old_path, new_path

    def apply(self):
        self.tree.apply_renames(self.generate_renames())
