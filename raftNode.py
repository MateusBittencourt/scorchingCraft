import Pyro5.core
import Pyro5.api
import Pyro5.server
import threading
import time
import threading
from multicast import MulticastService
from enum import Enum
from timer import Timer
from random import randrange
from concurrent.futures import ProcessPoolExecutor
from functools import partial

class State(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

PYRO_ID = "Raft.Node"
HEARTBEAT_TIMEOUT = 150

def request_vote(node, name, term):
    uri = node["URI"]
    node_proxy = Pyro5.api.Proxy(uri)
    try:
        return node_proxy.vote(name, term)
    except Pyro5.errors.PyroError as e:
        print(f"Error sending vote to {uri}: {e}")
        
def send_heartbeat(node, name, term):
    uri = node["URI"]
    node_proxy = Pyro5.api.Proxy(uri)
    try:
        node_proxy.heartbeat(name, term)
    except Pyro5.errors.PyroError as e:
        print(f"Error sending heartbeat to {uri}: {e}")

@Pyro5.api.expose
class RaftNode(object):
    def __init__(self, id):
        self.state = None
        self.name = f"{PYRO_ID}.{id}"
        self.votes = 0
        
        self.leader = None
        self.term = 0
        self.nodes = []
        
        self.name_server = Pyro5.api.locate_ns()
        self.daemon = Pyro5.api.Daemon()
        self.uri = self.daemon.register(self)
        self.daemon_thread = threading.Thread(target=self.daemon.requestLoop)
        self.daemon_thread.daemon = True
        self.daemon_thread.start()
    
        self.heartbeat_timer = Timer(HEARTBEAT_TIMEOUT, self.no_heartbeat)
        self.candidate_timer = Timer(randrange(150, 300), self.become_candidate)
        
        self.multicast_service = MulticastService(self.update_nodes)

        self.receive_thread = threading.Thread(target=self.multicast_service.receive_multicast)
        self.receive_thread.daemon = True
        self.receive_thread.start()

        time.sleep(1)
        self.multicast_service.send_multicast({"name": self.name, "URI": str(self.uri)})

        self.become_follower()
    
    def print_if(self, message):
        if (self.name == "Raft.Node.0"):
            print(message)
            
    def become_follower(self):
        self.state = State.FOLLOWER
        print(f"{self.name} became follower")
        self.heartbeat_timer.start()
        self.candidate_timer.stop()
    
    def become_leader(self):
        self.state = State.LEADER
        self.name_server.register(f"Leader.Term{self.term}", self.uri)
        print(f"{self.name} became leader")
        
        send_heartbeat_thread = threading.Thread(target=self.send_heartbeats)
        send_heartbeat_thread.daemon = True
        send_heartbeat_thread.start()
        send_heartbeat_thread.join()
    
    def become_candidate(self):
        self.state = State.CANDIDATE
        self.term += 1
        self.votes = 1
        print(f"{self.name} became candidate")
        self.request_votes()
    
    def vote(self, candidate, term):
        print(f"{self.name} received vote request from {candidate}")
        if term > self.term:
            self.term = term
            self.leader = candidate
            self.become_follower()
            return True
        return False
    
    def request_votes(self):
        print(f"{self.name} requesting votes")
        request_vote_partial = partial(request_vote, name=self.name, term=self.term)
        
        with ProcessPoolExecutor() as executor:
            results = executor.map(request_vote_partial, self.nodes)
            for result in results:
                print(f"Vote result: {result}")
                if result:
                    self.votes += 1
                    print(f"{self.name} received vote")
                
        if self.votes > len(self.nodes) + 1 / 2:
            self.become_leader()
        else:
            self.no_heartbeat()
        
    
    def heartbeat (self, leader, term):
        if leader == self.leader and term == self.term:
            self.heartbeat_timer.reset()
            self.candidate_timer.stop()
            #print(f"Received heartbeat from {Pyro5.api.current_context.client_sock_addr}")
        elif term > self.term:
            self.leader = leader
            self.term = term
            if self.state != State.FOLLOWER:
                self.become_follower()
            else:
                self.heartbeat_timer.reset()
                self.candidate_timer.stop()
                #print(f"Received heartbeat from {Pyro5.api.current_context.client_sock_addr}")
    
    def send_heartbeats(self):
        send_heartbeat_partial = partial(send_heartbeat, name=self.name, term=self.term)
        
        while self.state == State.LEADER:
            with ProcessPoolExecutor() as executor:
                executor.map(send_heartbeat_partial, self.nodes)
            time.sleep(HEARTBEAT_TIMEOUT/1500)
    
    def no_heartbeat(self):
        print(f"No heartbeat from {self.leader}")
        self.candidate_timer = Timer(randrange(150, 1000), self.become_candidate)
        self.candidate_timer.start()
        
    
    def update_nodes(self, nodes):
        if self.name != nodes["name"]:
            self.nodes.append(nodes)

if __name__ == "__main__":
    node = RaftNode(randrange(3000))