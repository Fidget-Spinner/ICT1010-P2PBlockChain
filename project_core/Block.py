"""
Class-based implementation of a single Block.
"""

from datetime import datetime

import json
import hashlib


class Block:
    def __init__(self, index: int, previous_hash: str, timestamp: float, data: str, own_hash: str = ""):
        """
        :param index: index of the current instance of the block
        :param previous_hash: value of the previous block's hash, 0 if genesis block
        :param timestamp: creation time of the block
        :param data: data contained inside the block
        """
        self.index = index  # int
        self.previous_hash = previous_hash  # str
        self.timestamp = timestamp  # float
        self.data = data  # str
        if own_hash:
            self._own_hash = own_hash
        else:
            self._own_hash = Block._find_own_hash(index, previous_hash, timestamp, data) # str

    def __str__(self):
        return \
            f"Index: {self.index} [{type(self.index)}], \n\
            previous_hash: {self.previous_hash}[{type(self.previous_hash)}], \n\
            timestamp: {self.timestamp} [{type(self.timestamp)}], \n\
            data: {self.data} [{type(self.data)}], \n\
            own_hash: {self._own_hash} [{type(self._own_hash)}]"

    def __repr__(self):
        return self.__str__()

    def get_list_repr(self):
        """ Get a list representation of a block's fields that can be used to initialise the block again via Block()"""
        return [self.index, self.previous_hash, self.timestamp, self.data, self.own_hash]

    @staticmethod
    def _find_own_hash(index: int, previous_hash: str, timestamp: float, data: str) -> str:
        """ Calculates own hash by concatenating index, previous_hash, timestamp, data and applying SHA256"""
        hash_this = (str(index) + str(previous_hash) + str(timestamp) + str(data)).encode("utf-8")
        return hashlib.sha256(hash_this).hexdigest()

    @property
    def own_hash(self):
        return self._own_hash

    @own_hash.setter
    def own_hash(self, value):
        self._own_hash = str(value)

    def pack_json(self, command):
        return json.dumps({"command": command, "data": self.get_list_repr()})

    @staticmethod
    def unpack_json(block_json):
        return json.loads(block_json)

    @classmethod
    def create_block_from_json(cls, packed_json) -> "Block":
        return Block(*cls.unpack_json(packed_json).get("data"))

    @staticmethod
    def generate_next_block(blockchain: ["Block", ], data: str) -> "Block":
        """ Creates the next Block for an array of blocks, given data """
        return Block(index=len(blockchain), previous_hash=blockchain[-1].own_hash if len(blockchain) > 0 else str(0), timestamp=datetime.timestamp(datetime.now()), data=data)


