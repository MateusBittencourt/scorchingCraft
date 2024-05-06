import raftNode
from concurrent.futures import ProcessPoolExecutor

NODES_NUMBER = 2

def start_nodes(id):
    print(f"Starting node {id}")
    node = raftNode.RaftNode(id)

if __name__ == "__main__":
    with ProcessPoolExecutor() as executor:
        results = executor.map(start_nodes, range(NODES_NUMBER))


