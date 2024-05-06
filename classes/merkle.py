import hashlib


class MerkleTree:
    def __init__(self, arr):
        self.arr = self.__hash_all(arr)
        self.root = self.__get_merkle_root(self.arr)

    def __hash_all(self, arr):
        return [self.__return_hash(x, False) for x in arr]

    def __get_merkle_root(self, arr):
        if len(arr) == 1:
            return self.__return_hash(arr[0])
        new_arr = []
        for i in range(0, len(arr), 2):
            combined_hashes = arr[i] + (arr[i + 1] if i + 1 < len(arr) else '')
            new_arr.append(self.__return_hash(combined_hashes))
        return self.__get_merkle_root(new_arr)

    def __return_hash(self, data, encode=True):
        if encode:
            return hashlib.sha256(data.encode()).hexdigest()

        return hashlib.sha256(data).hexdigest()

    def get_root(self):
        return self.root
