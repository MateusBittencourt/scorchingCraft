import Pyro5.core
import Pyro5.api
import Pyro5.server
from threading import Lock
from threading import Thread
from multicast import MulticastService
from enum import Enum
from timer import Timer
from random import randrange
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from dotmap import DotMap

PYRO_ID = "Raft.Node"
HEARTBEAT_INTERVAL = 200
HEARTBEAT_TIMEOUT = HEARTBEAT_INTERVAL * 2

class EventType(Enum):
    HEARTBEAT = 1
    VOTE = 2
    TIMEOUT = 3
    APPEND_ENTRIES = 4
    
class MessageType(Enum):
    REPLY = 1
    EVENT = 2

############################################################################################
# Class that represents the basic state of the node
class State():
    def __init__(self, raft_node):
        self.raft_node = raft_node

    def on_event(self, event):
        match event.messageType:
            case MessageType.EVENT:
                match event.type:
                    case EventType.HEARTBEAT:
                        return FollowerState(self.raft_node)
        return self
    
    def init_class(self):
        event = {
            "type": EventType.HEARTBEAT,
            "messageType": MessageType.EVENT
        }
        self.proxy_call(self.raft_node.uri, event)
    
    # def heartbeat (self, event_data):
    #     if event_data.term >= self.raft_node.term:
    #         try:
    #             self.heartbeat_timer.reset()
    #         except AttributeError:
    #             pass
    #         try:
    #             self.heartbeat_scheduler.stop()
    #         except AttributeError:
    #             pass
    #         try:
    #             self.stop_election_countdown()
    #         except AttributeError:
    #             pass
    #         if event_data.leader != self.raft_node.leader or event_data.term != self.raft_node.term:
    #             self.raft_node.leader = event_data.leader
    #             self.raft_node.term = event_data.term
    #             return FollowerState(self.raft_node)
    #     return self
    
    # def vote(self, event_data):
    #     print(f"{self.raft_node.name} received vote request from {event_data.candidate}")
    #     if event_data.term > self.raft_node.term:
    #         try:
    #             self.stop_election_countdown()
    #         except AttributeError:
    #             pass
    #         self.raft_node.term = event_data.term
    #         self.raft_node.leader = event_data.candidate
    #         node_proxy = Pyro5.api.Proxy(event_data.uri)
    #         reply = {
    #             "type": EventType.VOTE,
    #             "messageType": MessageType.REPLY,
    #             "data": {
    #                 "uri": str(self.raft_node.uri),
    #                 "term": self.raft_node.term,
    #                 "voter": self.raft_node.name
    #             }
    #         }
    #         try:
    #             node_proxy.on_event(reply)
    #         except Pyro5.errors.PyroError as e:
    #             print(f"Error sending vote to {event_data.uri}: {e}")
    #         return FollowerState(self.raft_node)
    #     return self

    def proxy_call(self, uri, event):
        proxy_thread = Thread(target=self.raft_node.proxy_call, args=(uri, event))
        proxy_thread.daemon = True
        proxy_thread.start()
        
    def __repr__(self):
        return self.name

############################################################################################
# Class that represents the follower state of the node
class FollowerState(State):
    def __init__(self, raft_node):
        super().__init__(raft_node)
        print(f"{self.raft_node.name} became follower")
        self.heartbeat_timer = Timer(HEARTBEAT_TIMEOUT, self.timeout_event)
        self.heartbeat_timer.start()
        
    def on_event(self, event):
        match event.messageType:
            case MessageType.EVENT:
                match event.type:
                    case EventType.HEARTBEAT:
                        self.heartbeat(event.data)
                    case EventType.VOTE:
                        self.vote(event.data)
                    case EventType.TIMEOUT:
                        self.heartbeat_timer.stop()
                        return CandidateState(self.raft_node)
                    case EventType.APPEND_ENTRIES:
                        self.append_entries(event.data)
            case MessageType.REPLY:
                match event.type:
                    case EventType.APPEND_ENTRIES:
                        self.append_entries_reply(event.data)
        return self
    
    def heartbeat (self, event_data):
        if event_data.term >= self.raft_node.term:
            self.heartbeat_timer.reset()
            if event_data.leader != self.raft_node.leader or event_data.term != self.raft_node.term:
                self.raft_node.leader = event_data.leader
                self.raft_node.term = event_data.term
    
    def timeout_event (self):
        print(f"{self.raft_node.name} timed out")
        event = {
            "type": EventType.TIMEOUT,
            "messageType": MessageType.EVENT
        }
        self.proxy_call(self.raft_node.uri, event)
    
    def vote(self, event_data):
        # print(f"{self.raft_node.name} received vote request from {event_data.candidate}")
        if event_data.term > self.raft_node.term:
            self.raft_node.term = event_data.term
            self.raft_node.leader = event_data.candidate
            reply = {
                "type": EventType.VOTE,
                "messageType": MessageType.REPLY,
                "data": {
                    "voter": self.raft_node.name,
                    "uri": str(self.raft_node.uri),
                    "term": self.raft_node.term
                }
            }
            self.proxy_call(str(event_data.uri), reply)

############################################################################################
# Class that represents the candidate state of the node
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
    
    def heartbeat (self, event_data):
        if event_data.term >= self.raft_node.term:
            self.stop_election_countdown()
            self.raft_node.leader = event_data.leader
            self.raft_node.term = event_data.term
            print(f"{self.raft_node.name} who is a candidate received heartbeat from {event_data.leader}")
            return FollowerState(self.raft_node)
        return self

    def election_countdown(self):
        self.stop_election_countdown()
        self.candidate_timer = Timer(randrange(150, 300), self.request_votes)
        self.candidate_timer.start()
    
    def stop_election_countdown(self):
        # print(f"{self.raft_node.name} stopping election countdown")
        try:
            self.candidate_timer.stop()
        except AttributeError:
            pass
        
    def request_votes(self):
        self.raft_node.term += 1
        self.raft_node.leader = self.raft_node.name
        
        # print(f"{self.raft_node.name} requesting votes")
        
        reply = {
            "type": EventType.VOTE,
            "messageType": MessageType.REPLY,
            "data": {
                "voter": self.raft_node.name,
                "uri": str(self.raft_node.uri),
                "term": self.raft_node.term
            }
        }
        
        event = {
            "type": EventType.VOTE,
            "messageType": MessageType.EVENT,
            "data": {
                "candidate": self.raft_node.name,
                "uri": str(self.raft_node.uri),
                "term": self.raft_node.term
            }
        }
        
        self.proxy_call(str(self.raft_node.uri), reply)
        for node in self.raft_node.nodes:
            self.proxy_call(node.uri, event)
    
    
    
    def receive_vote(self, event_data):
        self.raft_node.votes += 1
        # print(f"{self.raft_node.name} received vote from {event_data.voter}")
        if self.raft_node.votes > len(self.raft_node.nodes) + 1 / 2:
            # print(f"{self.raft_node.name} received majority of votes")
            self.stop_election_countdown()
            return LeaderState(self.raft_node)
        return self

    def vote(self, event_data):
        # print(f"{self.raft_node.name} received vote request from {event_data.candidate}")
        if event_data.term > self.raft_node.term:
            self.stop_election_countdown()
            self.raft_node.term = event_data.term
            self.raft_node.leader = event_data.candidate
            reply = {
                "type": EventType.VOTE,
                "messageType": MessageType.REPLY,
                "data": {
                    "voter": self.raft_node.name,
                    "uri": str(self.raft_node.uri),
                    "term": self.raft_node.term
                }
            }
            self.proxy_call(str(event_data.uri), reply)
            return FollowerState(self.raft_node)
        return self

############################################################################################
# Class that represents the leader state of the node
class LeaderState(State):
    def __init__(self, raft_node):
        super().__init__(raft_node)
        print(f"{self.raft_node.name} became leader")
        
        self.heartbeat_scheduler = Timer(HEARTBEAT_INTERVAL, self.send_heartbeat, continuous=True)
        self.heartbeat_scheduler.start()
        
        # self.raft_node.name_server.register(f"Leader.Term-{self.raft_node.term}", self.raft_node.uri)
        self.locate_ns_thread = Thread(target=self.register_ns)
        self.locate_ns_thread.daemon = True
        self.locate_ns_thread.start()
        
    def on_event(self, event):
        match event.messageType:
            case MessageType.EVENT:
                match event.type:
                    case EventType.HEARTBEAT:
                        return self.heartbeat(event.data)
                    case EventType.VOTE:
                        return self.vote(event.data)
                    case EventType.APPEND_ENTRIES:
                        self.append_entries(event.data)
            case MessageType.REPLY:
                match event.type:
                    case EventType.APPEND_ENTRIES:
                        self.append_entries_reply(event.data)
        return self
    
    def heartbeat (self, event_data):
        if event_data.term >= self.raft_node.term and event_data.leader != self.raft_node.leader:
            self.heartbeat_scheduler.stop()
            self.raft_node.leader = event_data.leader
            self.raft_node.term = event_data.term
            print(f"{self.raft_node.name} received heartbeat from {event_data.leader}")
            return FollowerState(self.raft_node)
        return self
    
    def send_heartbeat(self):
        event = {
            "type": EventType.HEARTBEAT,
            "messageType": MessageType.EVENT,
            "data": {
                "leader": self.raft_node.name,
                "uri": str(self.raft_node.uri),
                "term": self.raft_node.term
            }
        }
        for node in self.raft_node.nodes:
            # print(f"{self.raft_node.name} sending heartbeat to {node.name}")
            self.proxy_call(str(node.uri), event)
    
    def vote(self, event_data):
        # print(f"{self.raft_node.name} received vote request from {event_data.candidate}")
        if event_data.term > self.raft_node.term:
            print(f"{self.raft_node.name} received vote request from {event_data.candidate} for term {event_data.term}. Current term is {self.raft_node.term}")
            self.heartbeat_scheduler.stop()
            self.raft_node.term = event_data.term
            self.raft_node.leader = event_data.candidate
            reply = {
                "type": EventType.VOTE,
                "messageType": MessageType.REPLY,
                "data": {
                    "voter": self.raft_node.name,
                    "uri": str(self.raft_node.uri),
                    "term": self.raft_node.term
                }
            }
            self.proxy_call(str(event_data.uri), reply)
            print(f"{self.raft_node.name} received vote request from {event_data.candidate} for term {event_data.term}. Current term is {self.raft_node.term}")
            return FollowerState(self.raft_node)
        return self

    def register_ns(self):
        name = f"Leader.Term-{self.raft_node.term}"
        print(f"Registering leader {self.raft_node.name} as {name}")
        Pyro5.api.locate_ns().register(name, self.raft_node.uri)

############################################################################################
# Class that represents the Pyro5 node
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
                
        # Start the multicast listener on a separate thread
        self.multicast_service = MulticastService(self.update_nodes)
        self.receive_thread = Thread(target=self.multicast_service.receive_multicast)
        self.receive_thread.daemon = True
        self.receive_thread.start()

        # Send node info using multicast service
        self.multicast_service.send_multicast({"nodes": [{"name": self.name, "uri": str(self.uri)}], "type": "new_node", "name": self.name})
        
        # Initialize the state machine
        self.init_state()
        
        # Start the daemon loop, listening for incoming requests
        self.daemon.requestLoop()
        
    # This function is called when a new event is received
    def on_event(self, event):
        # print(f"{self.name} received event {event}")
        event = DotMap(event)
        event.type = EventType(event.type)
        event.messageType = MessageType(event.messageType)
        self.state = self.state.on_event(event)
        return None
        
    def init_state(self):
        self.state = State(self)
        self.state.init_class()

    def current_state(self):
        return self.state
    
    def update_nodes(self, reply):
        print(f"Received nodes update: {reply}")
        reply = DotMap(reply)
        for node in reply.nodes:
            if node not in self.nodes and node.name != self.name:
                self.nodes.append(node)
        print(f"Reply type: {reply.type}")
        if reply.type == "new_node" and reply.name != self.name:
            nodes_temp = []
            for node in self.nodes:
                nodes_temp.append(node.toDict())
            nodes_temp.append({"name": self.name, "uri": str(self.uri)})
            self.multicast_service.send_multicast({"nodes": nodes_temp, "type": "update_nodes", "name": self.name})
    
    def proxy_call(self, uri, event):
        node_proxy = Pyro5.api.Proxy(uri)
        try:
            node_proxy.on_event(event)
            # print(f"Sent event {event} to {uri}")
        except Pyro5.errors.PyroError as e:
            # print(f"Error sending vote to {uri}: {e}")
            pass
    

if __name__ == "__main__":
    node = RaftNode(randrange(3000))