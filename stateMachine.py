import Pyro5.core
import Pyro5.api
import Pyro5.server
from random import randrange
from threading import Thread
from timer import Timer
from typeEnums import MessageType, EventType
from variables import HEARTBEAT_TIMEOUT, HEARTBEAT_INTERVAL

class StateMachine():
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
    
    def heartbeat (self, event_data):
        if event_data.term >= self.raft_node.term:
            try:
                self.heartbeat_timer.reset()
            except AttributeError:
                pass
            try:
                self.heartbeat_scheduler.stop()
            except AttributeError:
                pass
            try:
                self.stop_election_countdown()
            except AttributeError:
                pass
            if event_data.leader != self.raft_node.leader or event_data.term != self.raft_node.term:
                self.raft_node.leader = event_data.leader
                self.raft_node.term = event_data.term
                if (self.raft_node.current_state() != FollowerState):
                    return FollowerState(self.raft_node)
        return self
    
    def vote(self, event_data):
        print(f"{self.raft_node.name} received vote request from {event_data.candidate}")
        if event_data.term > self.raft_node.term:
            try:
                self.stop_election_countdown()
            except AttributeError:
                pass
            try:
                self.heartbeat_scheduler.stop()
            except AttributeError:
                pass
            self.raft_node.term = event_data.term
            self.raft_node.leader = event_data.candidate
            reply = {
                "type": EventType.VOTE,
                "messageType": MessageType.REPLY,
                "data": {
                    "uri": str(self.raft_node.uri),
                    "term": self.raft_node.term,
                    "voter": self.raft_node.name
                }
            }
            self.proxy_call(str(event_data.uri), reply)
            if (self.raft_node.current_state() != FollowerState):
                return FollowerState(self.raft_node)
        return self

    def proxy_call(self, uri, event):
        proxy_thread = Thread(target=self.raft_node.proxy_call, args=(uri, event))
        proxy_thread.daemon = True
        proxy_thread.start()
        
    def __repr__(self):
        return self.name

class LeaderState(StateMachine):
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

    def append_entries(self, event_data):
        print(f"{self.raft_node.name} received append entries from client {event_data.client}")
        event = {
            "type": EventType.APPEND_ENTRIES,
            "messageType": MessageType.EVENT,
            "data": {
                "leader": self.raft_node.name,
                "uri": str(self.raft_node.uri),
                "term": self.raft_node.term,
                "log": {
                    "id": event_data.id,
                    "client": event_data.client,
                    "data": event_data.data,
                    "date": event_data.date,
                    "term": self.raft_node.term
                },
                "previous_log": self.raft_node.log[-1]
            }
        }
        for node in self.raft_node.nodes:
            self.proxy_call(str(node.uri), event)
    
    def append_entries_reply(self, event_data):
        print(f"{self.raft_node.name} received append entries reply from {event_data.leader}")
    
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

    def register_ns(self):
        name = f"Leader.Term-{self.raft_node.term}"
        print(f"Registering leader {self.raft_node.name} as {name}")
        Pyro5.api.locate_ns().register(name, self.raft_node.uri)

class CandidateState(StateMachine):
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

####################################################################################################
# Follower State
####################################################################################################
class FollowerState(StateMachine):
    def __init__(self, raft_node):
        super().__init__(raft_node)
        print(f"{self.raft_node.name} became follower")
        self.heartbeat_timer = Timer(HEARTBEAT_TIMEOUT, self.timeout_event)
        self.heartbeat_timer.start()
        self.waiting_log = {}
        
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
    
    def timeout_event (self):
        print(f"{self.raft_node.name} timed out")
        event = {
            "type": EventType.TIMEOUT,
            "messageType": MessageType.EVENT
        }
        self.proxy_call(self.raft_node.uri, event)
    
    def append_entries(self, event_data):
        print(f"{self.raft_node.name} received append entries from leader {event_data.leader}")                            
        previous_log = self.raft_node.log[-1]
        if event_data.previous_log != previous_log:
            print(f"{self.raft_node.name} log is inconsistent with leader's log")
            status = "inconsistent"
        else:
            self.waiting_log["id"] = event_data.log
            status = "consistent"
        reply = {
            "type": EventType.APPEND_ENTRIES,
            "messageType": MessageType.REPLY,
            "data": {
                "node": self.raft_node.name,
                "uri": str(self.raft_node.uri),
                "status": status
            }
        }
        self.proxy_call(str(event_data.uri), reply)