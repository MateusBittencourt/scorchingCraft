import Pyro5.core
import Pyro5.api
import Pyro5.server
import threading
import sched
import time
import threading
import asyncio
from multicast import MulticastService
from enum import Enum
from timer import Timer
from random import randrange
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from dotmap import DotMap

PYRO_ID = "Raft.Node"
HEARTBEAT_INTERVAL = 100
HEARTBEAT_TIMEOUT = HEARTBEAT_INTERVAL * 1.5

class EventType(Enum):
    HEARTBEAT = 1
    VOTE = 2
    NEW_LEADER = 3
    TIMEOUT = 4
    
class MessageType(Enum):
    REPLY = 1
    EVENT = 2


class State():
    def __init__(self, raft_node):
        self.raft_node = raft_node

    def on_event(self, event):
        """
        Handle events that are delegated to this State.
        """
        pass

    def __repr__(self):
        return self.name

class FollowerState(State):
    def __init__(self, raft_node):
        super().__init__(raft_node)
        print(f"{self.raft_node.name} became follower")
        self.heartbeat_timer = Timer(HEARTBEAT_TIMEOUT, self.timeout_event)
        self.heartbeat_timer.start()
        
    def on_event(self, event):
        if event.messageType == MessageType.EVENT:
            if event.type == EventType.HEARTBEAT:
                self.heartbeat(event.data)
            elif event.type == EventType.VOTE:
                self.vote(event.data)
            elif event.type == EventType.NEW_LEADER:
                self.new_leader(event.data)
            elif event.type == EventType.TIMEOUT:
                print(f"{self.raft_node.name} received timeout event")
                return CandidateState(self.raft_node)
        # Stay in the current state for all other events
        return self
    
    def timeout_event (self):
        node_proxy = Pyro5.api.Proxy(self.raft_node.uri)
        event = {
            "type": EventType.TIMEOUT,
            "messageType": MessageType.EVENT
        }
        node_proxy.on_event(event)
    
    def heartbeat (self, event_data):
        if event_data.leader == self.leader and event_data.term == self.raft_node.term:
            self.heartbeat_timer.reset()
        elif event_data.term > self.term:
            self.raft_node.leader = event_data.leader
            self.raft_node.term = event_data.term
    
    def vote(self, event_data):
        print(f"{self.name} received vote request from {event_data.candidate}")
        if event_data.term > self.term:
            self.term = event_data.term
            self.leader = event_data.candidate
            node_proxy = Pyro5.api.Proxy(event_data.URI)
            reply = {
                "type": EventType.REPLY,
                "messageType": MessageType.REPLY
            }
            try:
                node_proxy.on_event(reply)
            except Pyro5.errors.PyroError as e:
                print(f"Error sending vote to {event_data.URI}: {e}")

class CandidateState(State):
    def __init__(self, raft_node):
        super().__init__(raft_node)
        print(f"{self.raft_node.name} became candidate")
        self.election_countdown()
    
    def on_event(self, event):
        match event.messageType:
            case MessageType.EVENT:
                match event.type:
                    case EventType.VOTE:
                        return self.vote(event.data)
                        
            case MessageType.REPLY: 
                match event.type:
                    case EventType.VOTE:
                        return self.receive_vote(event.data)
        return self
    
    def election_countdown(self):
        try:
            self.candidate_timer.stop()
        except AttributeError:
            pass
        self.candidate_timer = Timer(randrange(150, 300), self.request_votes)
        self.candidate_timer.start()
        
    def request_votes(self):
        self.raft_node.term += 1
        self.raft_node.leader = self.raft_node.name
        
        print(f"{self.raft_node.name} requesting votes")
        
        reply = {
            "type": EventType.VOTE,
            "messageType": MessageType.REPLY,
            "data": {
                "URI": str(self.raft_node.uri),
                "term": self.raft_node.term,
                "voter": self.raft_node.name
            }
        }
        node_proxy = Pyro5.api.Proxy(str(self.raft_node.uri))
        try:
            node_proxy.on_event(reply)
        except Pyro5.errors.PyroError as e:
            print(f"Error sending vote to {node.URI}: {e}")
        
        event = {
            "type": EventType.VOTE,
            "messageType": MessageType.EVENT,
            "data": {
                "candidate": self.raft_node.name,
                "URI": str(self.raft_node.uri),
                "term": self.raft_node.term
            }
        }
        for node in self.raft_node.nodes:
            node_proxy = Pyro5.api.Proxy(node.URI)
            print(f"Requesting vote from {node.URI}")
            try:
                node_proxy.on_event(event)
            except Pyro5.errors.PyroError as e:
                print(f"Error sending vote to {node.URI}: {e}")
    
    def vote(self, event_data):
        print(f"{self.raft_node.name} received vote request from {event_data.candidate}")
        if event_data.term > self.raft_node.term:
            self.candidate_timer.stop()
            self.raft_node.term = event_data.term
            self.raft_node.leader = event_data.candidate
            node_proxy = Pyro5.api.Proxy(event_data.URI)
            reply = {
                "type": EventType.VOTE,
                "messageType": MessageType.REPLY,
                "data": {
                    "URI": str(self.raft_node.uri),
                    "term": self.raft_node.term,
                    "voter": self.raft_node.name
                }
            }
            try:
                node_proxy.on_event(reply)
            except Pyro5.errors.PyroError as e:
                print(f"Error sending vote to {event_data.URI}: {e}")
            return FollowerState(self.raft_node)
        return self        
    
    def receive_vote(self, event_data):
        self.raft_node.votes += 1
        print(f"{self.raft_node.name} received vote from {event_data.voter}")
        if self.raft_node.votes > len(self.raft_node.nodes) + 1 / 2:
            print(f"{self.raft_node.name} received majority of votes")
            return LeaderState(self.raft_node)
        else:
            self.election_countdown()
        return self

class LeaderState(State):
    def __init__(self, raft_node):
        super().__init__(raft_node)
        print(f"{self.raft_node.name} became leader")
        
        
    def on_event(self, event):
        pass    
    
    def send_heartbeats(self):
        print(f"{self.raft_node.name} sending heartbeats")
        # event = {
        #     "type": EventType.HEARTBEAT,
        #     "messageType": MessageType.EVENT,
        #     "data": {
        #         "leader": self.raft_node.name,
        #         "URI": str(self.raft_node.uri),
        #         "term": self.raft_node.term
        #     }
        # }
        # for node in self.raft_node.nodes:
        #     node_proxy = Pyro5.api.Proxy(node.URI)
        #     try:
        #         node_proxy.on_event(event)
        #     except Pyro5.errors.PyroError as e:
        #         print(f"Error sending vote to {node.URI}: {e}")

@Pyro5.api.expose
class RaftNode(object):
    def __init__(self, id):
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
        
        self.multicast_service = MulticastService(self.update_nodes)

        self.receive_thread = threading.Thread(target=self.multicast_service.receive_multicast)
        self.receive_thread.daemon = True
        self.receive_thread.start()

        time.sleep(1)
        self.multicast_service.send_multicast({"name": self.name, "URI": str(self.uri)})
        
        self.state = FollowerState(self)
        
        self.daemon.requestLoop()
        
        # while True:
        #     pass
    
    def on_event(self, event):
        print(f"{self.name} received event {event}")
        event = DotMap(event)
        event.type = EventType(event.type)
        event.messageType = MessageType(event.messageType)
        self.state = self.state.on_event(event)

    def current_state(self):
        return self.state
    
    def update_nodes(self, nodes):
        nodes = DotMap(nodes)
        if self.name != nodes.name:
            self.nodes.append(nodes)
    

if __name__ == "__main__":
    node = RaftNode(randrange(3000))