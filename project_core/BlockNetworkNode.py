"""
Blockchain network protocol implemented using TCP and asyncio transports/streamreaders.

Requirements:
Demonstrate the P2P communication among three nodes/terminals
1.Synchronize the blockchain among all nodes
2.Query and receive the response of the latest block between two nodes
3.Query and receive the response of the timestamp a particular block

Overall Behaviour:
1. All network nodes run a single TCP server on a designated port (currently 4040).
2. All network nodes query the whole network periodically to synchronise itself.
3. When a new peer joins, it will send “queryall” to all other servers.
4. When a network node encounters a block that has an index larger than the current known block,
   it appends that block to its own blockchain as long as the index is just 1 more than its current last.
5. Automatically learn of new IPs added to a network, and add that IP to its own list of known IP.


Protocol details:
Header:

Length/B    Desc        Type
12          Command     char[12]
~           data        binary

Message protocol (serialised header for sending):
 JSON serialised: {command: "command",     // a command indicating what the message is for
                data: [index, hash...] // data can contain various things depending on command, usually blockchain list
                }

Command list:
queryall:   Query all blocks
getlatest:  get latest block on a node
getblock:   get specific block on a node

(for statuses)
success:    Used in response to any of the above 3 to indicate successful operation.
notfound:  Used in response to any of the above 3 to indicate a block does not exist.

Eg:
Query:
{command: “queryall”,
data:[]}

Response:
{command: “success”,
data:[Block0, Block1, Block2...]}

Eg:
Node 1                              Node 2
        -----> Query All Blocks
        <----- Send All Blocks

        -----> Query Latest Blocks
        <----- Send Latest Blocks

        -----> Query Timestamp of block
        <----- Send timestamp of a block

Technical details:

1. Each time a node wants to connect to another node, it shall use a tcp client to connect to the other node's server.
2. All nodes have tcp servers running on them.



"""
from .Block import Block
from itertools import compress

import asyncio
import json
import logging
import sys

# check for correct python version
if not (sys.version_info[0] == 3 and sys.version_info[1] >= 8): # >= python 3.8
    raise Exception(
        "Must be using Python 3.8 or higher due to named expression operator ':=' usage and asyncio features"
    )

DEFAULT_PORT = 4040
#  Blockchain protocol headers

GET_ALL = "queryall"
GET_LATEST = "getlatest"
GET_BLOCK = "getblock"

#  Control headers
SUCCESS = "success"
NOT_FOUND = "notfound"
CLOSE_CONN = "byebye"

# First block in the block chain
# GENESIS_BLOCK = Block(0, str(0),  datetime.timestamp(datetime.now()), "fidgetspinner")
GENESIS_BLOCK = None

""" General Purpose functions """


def _print_debug_info(writer, client_type: str, msg: str, rcv=True) -> None:
    """Log connection connection info in the format
    [(person) (ip addr, port no): Incoming/outgoing from/to (other person): (data received)]
    :param writer: an asyncio StreamWriter object or BaseTransport.transport to get socket info from
    :param client_type: the name of the receiving person that you wish to give
    :param msg: actual decoded(UTF-8) data coming in
    :param rcv: is the person receiving/sending this data?
    """
    own_addr, remote_addr = get_conn_info(writer)
    if rcv:
        logging.log(logging.INFO, f"[{client_type} {own_addr}]: Incoming from {remote_addr}: {msg}")
    else:
        logging.log(logging.INFO, f"[{client_type} {own_addr}]: Outgoing to {remote_addr}: {msg}")


def get_conn_info(writer: asyncio.StreamWriter) -> (str, str):
    """ own ip address and port, remote(incoming conn) ip address """
    return writer.get_extra_info("sockname"), writer.get_extra_info("peername")


class ClientProtocol(asyncio.Protocol):
    """ Client Transport Factory """
    def __init__(self, command: str, index: int or None, block_chain: [Block, ]):
        """
        :param command: the command to query
        :param index: optional, only needed if the command is GET_BLOCK, else is None
        :param block_chain: BlockNetworkNode's block chain
        """
        self.command = command
        self.index = index
        self.block_chain = block_chain

    def connection_made(self, transport):
        """ Callback that exists in parent class for handling behaviour when a connection is established """
        # print(transport.get_extra_info("sockname"), "talking")
        message = BlockNetworkNode.pack_query(self.command, self.index)
        transport.write(message.encode("utf-8"))
        self.transport = transport

    def data_received(self, reply):
        """ Callback that exists in parent class for handling behaviour when data is received"""
        if reply_data := json.loads(reply.decode("utf-8")):
            self._client_handle_reply(reply_data.get("command"), reply_data.get("data"), self.block_chain)

    def connection_lost(self, exc):
        """ Callback that exists in parent class for handling behaviour when data is lost"""
        pass

    def _client_handle_reply(self, incoming_cmd: str, incoming_data: [Block, ], own_block_chain: [Block, ]):
        """
        Private helper that responds to incoming_cmd from incoming data, and manages/updates BlockNetworknode's own blockchain.
        """
        if incoming_cmd == CLOSE_CONN:
            pass
        elif incoming_cmd == SUCCESS:
            _print_debug_info(self.transport, "CLIENT", f"UPDATING OWN BLOCKCHAIN: {incoming_data}", rcv=True)
            if self.command == GET_LATEST or self.command == GET_BLOCK:
                print(f"[CLIENT {self.transport.get_extra_info('sockname')[0]}] Received block(s) {incoming_data}")
            for block in incoming_data:
                # if block index in current chain; ie its an existing block, update it
                if (index := block[0]) < len(own_block_chain):
                    #logging.log(logging.INFO, f"OVERWRITING {block}")
                    own_block_chain[index] = Block(*block)  # overwrite the existing block
                else:
                    #logging.log(logging.INFO, f"APPENDING {block}")
                    own_block_chain.append(Block(*block))  # lengthen the blockchain
        return own_block_chain


class ServerProtocol(asyncio.Protocol):
    """ Server Transport Factory """
    peers = {}
    # global peer dict that stores peer data across transport instances, why multiple instance persistence
    # is needed is explained inside __init__
    # format: {server_ip:set(tuple(peer_ip, peer_port), )}

    def __init__(self, server_ip: str, server_port: int, block_chain: [Block, ], peer_list: {(str, int), } or None):
        """ Mildly confusing note: a new transport factory seems to be called every time a new connection is made. So
        this is not called once upon start_server/create_server, but multiple times over the lifetime of a server.

        :param server_ip: ipv4 addr of the server to listen on
        :param server_port: port no. of the server to listen on
        :param block_chain: BlockNetworknode's block chain
        :param peer_list: list of peers, optional at the start
        """
        self.server_ip = server_ip
        self.server_port = server_port
        self._block_chain = block_chain
        if temp := ServerProtocol.peers.get(server_ip):
            self.peer_list = temp
        else:
            self.peer_list = peer_list
            ServerProtocol.peers[server_ip] = peer_list
        #save_to_file(server_ip, self.peer_list)

    def connection_made(self, transport):
        """ Callback that exists in parent class for handling behaviour when a connection is established """
        peername = transport.get_extra_info("peername")
        #print(transport.get_extra_info("peername"))
        self._update_peer_list(*peername)
        self.transport = transport

    def data_received(self, raw_data):
        """ Callback that exists in parent class for handling behaviour when data is received"""
        if data := json.loads(raw_data.decode("utf-8")):
            ret_data = self._server_handle_queries(cmd := data.get("command"), self._block_chain, data.get("data"))
            if cmd == GET_BLOCK or cmd == GET_LATEST:
                print(f"[SERVER {self.server_ip}] Response to {cmd}: {ret_data}")
            self.transport.write(ret_data.encode("utf-8"))
            self.transport.close()

    def _update_peer_list(self, peer_ip, peer_port):
        """ Update ServerProtocol's attribute 'peers'"""
        if peer_ip != self.server_ip:
            ServerProtocol.peers[self.server_ip].add((peer_ip, DEFAULT_PORT))
        self.peer_list = ServerProtocol.peers[self.server_ip]

    def _server_handle_queries(self, command: str, block_chain: [Block, ], index=None) -> str:
        """ Private helper for the server. Decides what to do from incoming command.

        :param command: incoming command, one of GET_ALL, GET_LATEST, GET_BLOCK, SUCCESS, NOT_FOUND, CLOSE_CONN
        :param block_chain: the Block array
        :param index: optional, only required if you issue the GET_BLOCK command which queries a specific block
        :return: the data written out in response to the query
        """
        # print_debug_info(writer, "SERVER", command, rcv=True)
        command_map = {
            GET_ALL: slice(None, None),
            GET_LATEST: slice(-1, None),
            GET_BLOCK: slice(index, index + 1) if isinstance(index, int) else "",
            SUCCESS: None,
            NOT_FOUND: None,
            CLOSE_CONN: "deadbeef"
        }
        chain_slice = command_map.get(command)
        if command == GET_BLOCK and isinstance(index, int):
            if not 0 < index < len(block_chain):
                chain_slice = "deadbadd"
        return self._handle_query_x(block_chain, chain_slice)

    @staticmethod
    def _handle_query_x(block_chain: [Block, ], chain_slice: slice or str) -> str:
        """ Returns art or all of the blockchain using list slices.

        :param block_chain: BlockNetworkNode's block chain
        :param chain_slice: dictates which part of the block_chain array to write to the StreamWriter and return
        """
        if chain_slice == "deadbadd":
            data = BlockNetworkNode.pack_query(NOT_FOUND)
        else:
            data = BlockNetworkNode.pack_some_blocks(SUCCESS, block_chain[chain_slice])
        return data


class BlockNetworkNode:
    """ A single, self-managing p2p node on the 'block chain' network. This can fully simulate a host machine."""
    def __init__(self, server_ip: str, server_port: int, peer_list: [(str, int), ]=None, block_chain=None, ioloop=None):
        """
        :param server_ip: node's own ip address
        :param server_port: node's server's own port number
        :param peer_list: list of peers on the network, needs at least one to start. peers in format (ip_addr, port)
        :param block_chain: optional, only if you want to pass in a pre-existing block chain
        """
        self.server_instance = None  # (asyncio.Server)
        self.ioloop = ioloop
        self._clients = []  # list of tcp clients the current node has
        if peer_list:
            self.peer_list = set([peer for peer in peer_list if peer != (server_ip, server_port)][:])
            logging.log(logging.INFO, f"{server_ip}'s peers: {self.peer_list}")
            print(f"{server_ip}'s peers: {self.peer_list}")
            # save_to_file(server_ip, peer_list)
        else:
            self.peer_list = set()
        if not block_chain:
            self._block_chain = [GENESIS_BLOCK, ] if GENESIS_BLOCK else []  # block chain usually needs a genesis block
        else:
            self._block_chain = block_chain
            # second block for testing
            # self._block_chain.append(Block.generate_next_block(self._block_chain, "fidgetcube"))
        self.server_ip = server_ip
        self.server_port = server_port

    @property
    def clients(self) -> list:
        return self._clients

    @property
    def peer_list(self) -> set:
        return self._peer_list

    @peer_list.setter
    def peer_list(self, new_peer_list):
        self._peer_list = new_peer_list

    @property
    def block_chain(self) -> [Block, ]:
        return self._block_chain

    @block_chain.setter
    def block_chain(self, new_chain):
        self._block_chain = new_chain

    """ Block chain methods """

    def add_block(self, data: str = "", block: Block = None) -> None:
        if data and block:
            raise Exception("Data and block are mutually exclusive. Cannot have both at once.")
        if data:
            self._block_chain.append(Block.generate_next_block(self._block_chain, data))
        if block:
            self._block_chain.append(block)

    def delete_block(self, index: int) -> None:
        del self._block_chain[index:]  # deletes all blocks after block_chain since their hashes will be invalid

    def list_all_blocks_data(self, blockchain: ["Block", ] = "") -> list:
        blockchain = blockchain or self._block_chain
        return [block.data for block in blockchain]

    async def validate_block_chain(self) -> bool:
        bad_blocks = compress(range(len(chain := self.block_chain)), [not block.validate_block() for block in chain])
        index = None
        try:
            # discarding everything after first bad block
            index = next(bad_blocks)
        except StopIteration:
            pass
        self._block_chain = self._block_chain[:index]
        print(f"Bad blocks: {index or 'None'}")
        return not all(bad_blocks)

    """ TCP P2P Methods """
    """ Data preparation methods """
    @staticmethod
    def pack_some_blocks(command, operate_on_block_chain) -> str:
        """ Prepares the whole block chain for sending, requires a command, can operate on a non-self blockchain

        :param command: one of the blockchain network header commands: GET_BLOCK, GET_LATEST, etc.
        :param operate_on_block_chain: the block chain to operate on
        """
        data = [block.get_list_repr() for block in operate_on_block_chain]
        return json.dumps({"command": command, "data": data})

    @staticmethod
    def pack_query(command, index=None) -> str:
        return json.dumps({"command": command, "data": [] if index is None else index}) # serialize to json str

    """ P2P client methods """

    async def send_query(self, command: str, target_ip: str, target_port: int=DEFAULT_PORT, index=None) -> None:
        """ Top level coroutine mean to be used by users. Will craft and send a query to server target_ip:target_port"""
        loop = asyncio.get_running_loop()
        try:
            await loop.create_connection(lambda: ClientProtocol(command, index, self._block_chain), target_ip, target_port)
        except ConnectionRefusedError:
            # sometimes the other side hasn't set up a server yet, so just ignore it
            pass
        # print_debug_info(writer, "CLIENT", reply_data, rcv=True)

    """ P2P server methods """

    async def setup_server(self):
        """ Returns a server object. Use to start the server """
        loop = asyncio.get_running_loop()
        self.server_instance = await loop.create_server(
            lambda: ServerProtocol(self.server_ip, self.server_port, self._block_chain, self._peer_list),
            self.server_ip, self.server_port, start_serving=True)
        print(f"SET A SERVER {self.server_ip} UP")
        return self.server_instance

    async def teardown(self):
        """ Stop the server """
        self.server_instance.close()
        await self.server_instance.wait_closed()

    async def _remove_dead_clients(self):
        """ Removes inactive/closed clients from node's client list """
        for index, client in enumerate(self._clients):
            if client[1].is_closing():
                del self._clients[index]

    async def behaviour(self, peer_list=None):
        """ The general behaviour: on first connection it will query others, then periodically update itself"""
        logging.log(logging.INFO, f"{self.server_ip}'s CURRENT blockchain: {self.list_all_blocks_data()}")
        if peer_list:
            self._peer_list = set([peer for peer in peer_list if peer != (self.server_ip, self.server_port)])
        else:
            self._peer_list = ServerProtocol.peers.get(self.server_ip) or self._peer_list
        await asyncio.gather(*[self.send_query(GET_ALL, peer_ip, peer_port) for peer_ip, peer_port in self._peer_list])
        print(f"{self.server_ip}'s NEW blockchain: {self.list_all_blocks_data()}")
        await self._remove_dead_clients()
        logging.log(logging.INFO, f"{self.server_ip}'s active clients: {len(self._clients)}")
        logging.log(logging.INFO, self.peer_list)



