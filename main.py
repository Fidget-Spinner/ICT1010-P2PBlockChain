from project_core import Block, BlockNetworkNode
from concurrent.futures import ThreadPoolExecutor

import signal
import asyncio
import logging
import platform
import sys

if not (sys.version_info[0] == 3 and sys.version_info[1] >= 8):
    assert False, "Must be using Python 3.8 or higher due to named expression operator ':=' usage and asyncio features"

# overall script logging mode
logging.basicConfig(level=logging.WARNING)

# how often to poll other nodes ( in seconds )
POLL_INTERVAL = 1

DEFAULT_PORT = 4040


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


async def poll_forever(server_ip, peer_list, do_things: dict, server_port=DEFAULT_PORT):
    node = BlockNetworkNode(server_ip, server_port, peer_list=peer_list)
    server = await node.setup_server()
    iterations = 0
    async with server:
        while True:
            if things_list := do_things.get(iterations):
                for thing in things_list:
                    if "add" in thing:
                        print(f"ADDING NEW BLOCK TO {server_ip}")
                        node.add_block(thing.split(":")[1])
            await node.behaviour()
            await asyncio.sleep(POLL_INTERVAL)
            iterations += 1
            if iterations >= 10:
                await _graceful_shutdown(signal.SIGTERM)
                break


async def main():
    peer_list = [("127.0.0.3", DEFAULT_PORT), ("127.0.0.2", DEFAULT_PORT)]
    todo1 = {
        1: ["add:fidgetcube", "add:figetstick"]
    }
    todo3 = {
        3: ["add:fidgetdoughnut", "add:fidgetgame"]
    }
    asyncio.ensure_future(asyncio.gather(poll_forever("127.0.0.3", peer_list, todo1), poll_forever("127.0.0.2", peer_list, dict())))
    await asyncio.sleep(1)
    print("Adding a new node 127.0.0.1")
    await poll_forever("127.0.0.1", peer_list, todo3)


if __name__ == "__main__":
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(_graceful_shutdown(signal.SIGTERM))
        loop.close()





  
