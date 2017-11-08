import os

__metaclass__ = type


class MemoryTree:
    """Represents a filesystem tree."""

    def __init__(self):
        self._content = {}

    def path_to_id(self, path):
        normpath = os.path.normpath(path)
        if normpath.startswith('..'):
            raise ValueError('Path outside tree.')
        return 'e-{}'.format(normpath)

    def id_to_path(self, file_id):
        if file_id[:2] != 'e-':
            raise ValueError('Invalid path.')
        return file_id[2:]

    def write_content(self, path, strings):
        self._content[path] = ''.join(strings)

    def read_content(self, path):
        return self._content[path]

    def rename(self, old_path, new_path):
        self._content[new_path] = self._content.pop(old_path)


class TreeTransform:

    def __init__(self, tree):
        self.tree = tree
        self._name_info = {}

    def acquire_existing_id(self, path):
        file_id = self.tree.path_to_id(path)
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
            return self.tree.id_to_path(file_id)
        if parent_id == 'e-.':
            return name
        parent_path = self.get_final_path(parent_id)
        return os.path.join(parent_path, name)


    def apply(self):
        for file_id, (parent_id, name) in self._name_info.items():
            old_path = self.tree.id_to_path(file_id)
            new_path = self.get_final_path(file_id)
            self.tree.rename(old_path, new_path)
