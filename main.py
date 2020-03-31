from project_core import Block, BlockNetworkNode
from datetime import datetime

import signal
import asyncio
import logging
import sys
import platform


if not (sys.version_info[0] == 3 and sys.version_info[1] >= 8):
    assert False, "Must be using Python 3.8 or higher due to named expression operator ':=' usage and asyncio features"

# overall script logging mode
logging.basicConfig(level=logging.WARNING)

# Commands
GET_ALL = "queryall"
GET_LATEST = "getlatest"
GET_BLOCK = "getblock"

# how often to poll other nodes ( in seconds )
POLL_INTERVAL = 1
# default port to listen on
DEFAULT_PORT = 4040
# maximum iterations before killing loop
MAX_ITER = 10


# the first block in the block chain, change the "fidgetspinner" value to change the data in the block
#GENESIS_BLOCK = Block(0, str(0),  datetime.timestamp(datetime.now()), "fidgetspinner")


async def _graceful_shutdown(sig) -> None:
    """ Gracefully terminate the sites and cleanup App runner, then finishes all pending coroutines and stops loop."""
    print(f'Received {sig.name}. Beginning graceful shutdown...', end="")
    # list of all other tasks except this coroutine
    tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
    list(map(lambda task: task.cancel(), tasks))    # cancel all the tasks
    results = await asyncio.gather(*tasks, return_exceptions=True)
    print(f'Finished awaiting cancelled tasks, results: {results}...', end="")
    await asyncio.sleep(1)  # to really make sure everything else has time to stop
    print(f"Done!")


async def _handle_node_todo(server_ip: str, node: BlockNetworkNode, do_things: dict, iterations: int):
    if things_list := do_things.get(iterations):
        for thing in things_list:
            thing = thing.split(":")
            if "add" in (case := thing[0]):
                """ Add a legitimate block to the block chain """
                print(f"ADDING NEW BLOCK \"{thing[1]}\" TO {server_ip}")
                node.add_block(thing[1])
            elif "cmd" in case:
                """ Execute certain p2p command """
                if len(thing) > 3:
                    await node.send_query(thing[1], thing[2], index=int(thing[3]))
                else:
                    await node.send_query(thing[1], thing[2])
            elif "validate" in case:
                """ Validate the block chain """
                await node.validate_block_chain()
            elif "bogus" in case:
                """ Add a bogus block to the block chain"""
                print("ADDING BOGUS BLOCK BAD")
                node.add_block(block=Block(1e7, "bad", 10.0, "bad", "bad"))  # clearly a bogus block
            elif "teardown" in case:
                await node.teardown()
                return -1


async def poll_forever(server_ip: str, peer_list, do_things: dict, server_port=DEFAULT_PORT):
    node = BlockNetworkNode(server_ip, server_port, peer_list=peer_list)
    server = await node.setup_server()
    iterations = 0
    await _handle_node_todo(server_ip, node, do_things, iterations)
    async with server:  # will stop server upon exit
        while True:
            await node.behaviour()
            await asyncio.sleep(POLL_INTERVAL)
            iterations += 1
            if await _handle_node_todo(server_ip, node, do_things, iterations) == -1:
                break
            if iterations >= MAX_ITER:
                break


async def main():
    peer_list = [("127.0.0.3", DEFAULT_PORT), ("127.0.0.2", DEFAULT_PORT)]
    # This is what the nodes will do at a certain iteration number.
    # The format is {iteration_num: list_of_things_to_do}
    # For demo purposes only

    # One of the initial nodes in the network
    todo3 = {
        0: ["add:fidgetspinner"],  # Create genesis block
        4: ["add:fidgetcube"], # at Iter 2 add a new block with this data
        5: ["add:figetstick"],  # At iter 3, add a new block
        6: ["teardown:teardown"]  # stop server
    }

    todo2 = {
        6: ["teardown:teardown"]
    }
    # For the third node added later on. To demonstrate that the other nodes have learnt of it and can update themselves
    todo1 = {
        2: ["add:fidgetdoughnut"], #  at iter 4, add a new block
        5: [f"cmd:{GET_LATEST}:127.0.0.3"], # at iter 5, execute cmd GET_LATEST, querying 127.0.0.3
        6: [f"cmd:{GET_BLOCK}:127.0.0.3:2"],  # at iter 6, execute cmd GET_BLOCK, querying 127.0.0.3, index=2
        7: ["validate:validate"], # validating block chain
        8: ["bogus:bogus"], # throw in some random bogus block
        9: ["validate:validate"],
    }

    # Initially, create a network with only 2 nodes inside.
    asyncio.ensure_future(asyncio.gather(poll_forever("127.0.0.3", peer_list, todo3), poll_forever("127.0.0.2", peer_list, todo2)))
    await asyncio.sleep(1)
    # Add at third node to the network
    print("Adding a new node(127.0.0.1) to network")
    await poll_forever("127.0.0.1", peer_list, todo1)
    # Clean up
    # test all 3 functionality

    await _graceful_shutdown(signal.SIGTERM)

if __name__ == "__main__":
    if platform.system() == "Windows":
        # SelectorEventLoop was used by default in python 3.7,
        # but the default changed to the more unstable ProactorEventLoop in python 3.8
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(_graceful_shutdown(signal.SIGTERM))
        loop.close()





  
