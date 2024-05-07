import Pyro5.core
import Pyro5.api
import Pyro5.server
from random import randrange
from threading import Thread

from dotmap import DotMap
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
                    case EventType.INIT:
                        return FollowerState(self.raft_node)
        return self
    
    def init_class(self):
        event = {
            "type": EventType.INIT,
            "messageType": MessageType.EVENT
        }
        self.proxy_call(self.raft_node.uri, event)

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
        
        self.append_confirmation = DotMap()
        self.append_waiting = DotMap()
        
        self.heartbeat_scheduler = Timer(HEARTBEAT_INTERVAL, self.send_heartbeat, continuous=True)
        self.heartbeat_scheduler.start()
        
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
        if event_data.term >= self.raft_node.term:
            self.heartbeat_timer.reset()
            if event_data.leader != self.raft_node.leader or event_data.term != self.raft_node.term:
                self.raft_node.leader = event_data.leader
                self.raft_node.term = event_data.term
                
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

    def append_entries(self, event_data):
        print(f"{self.raft_node.name} received append entries from client {event_data.client}")
        if len(self.raft_node.log) == 0:
            previous_log = None
        else:
            previous_log = self.raft_node.log[-1]
        
        log = {
            "id": event_data.id,
            "client": event_data.client,
            "data": {
                "operation": event_data.data.operation,
                "arguments": event_data.data.arguments,
                "success": event_data.data.success
            },
            "date": event_data.date,
            "term": self.raft_node.term
        }
        
        event = {
            "type": EventType.APPEND_ENTRIES,
            "messageType": MessageType.EVENT,
            "data": {
                "leader": self.raft_node.name,
                "uri": self.raft_node.uri,
                "term": self.raft_node.term,
                "log": log,
                "previous_log": previous_log
            }
        }
        
        self.append_waiting[event_data.id] = log
        self.append_confirmation[event_data.id] = 0
        for node in self.raft_node.nodes:
            print(f"{self.raft_node.name} sending append entries to {node.name}")
            self.proxy_call(node.uri, event)
            print(f"{self.raft_node.name} sent append entries to {node.name}")
        self.check_append_entries_reply(event_data.id, "consistent")
    
    def append_entries_reply(self, event_data):
        print(f"{self.raft_node.name} received append entries reply from {event_data.node}")
        self.check_append_entries_reply(event_data.logId, event_data.status)
    
    def check_append_entries_reply(self, logId, status):
        if logId in self.append_waiting:
            if status == "consistent":
                self.append_confirmation[logId] += 1
            if self.append_confirmation[logId] > len(self.raft_node.nodes) + 1 / 2:
                self.raft_node.log.append(self.append_waiting[logId])
                
                print(f"{self.raft_node.name} log is now {self.raft_node.log}")
                
                self.append_waiting.pop(logId)
                self.append_confirmation.pop(logId)
                
                event = {
                    "type": EventType.COMMIT_ENTRIES,
                    "messageType": MessageType.EVENT,
                    "data": {
                        "leader": self.raft_node.name,
                        "uri": str(self.raft_node.uri),
                        "term": self.raft_node.term,
                        "logId": logId
                    }
                }
                
                for node in self.raft_node.nodes:
                    self.proxy_call(str(node.uri), event)
        
    
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
        metadata = {
            "Leader" : "",
            f"{self.raft_node.term}" : "" 
        }
        print(f"Registering leader {self.raft_node.name}")
        Pyro5.api.locate_ns().register(name, self.raft_node.uri, metadata=metadata)

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
                    case EventType.HEARTBEAT:
                        return self.heartbeat(event.data)
                    case EventType.APPEND_ENTRIES:
                        self.append_entries(event.data)
                        
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
        self.election_countdown()
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
    
    def append_entries(self, event_data):
        print(f"{self.raft_node.name} received append entries while in candidate state")
        
    
####################################################################################################
# Follower State
####################################################################################################
class FollowerState(StateMachine):
    def __init__(self, raft_node):
        super().__init__(raft_node)
        print(f"{self.raft_node.name} became follower")
        self.heartbeat_timer = Timer(HEARTBEAT_TIMEOUT, self.timeout_event)
        self.heartbeat_timer.start()
        self.append_waiting = DotMap()
        
    def on_event(self, event):
        match event.messageType:
            case MessageType.EVENT:
                match event.type:
                    case EventType.HEARTBEAT:
                        self.heartbeat(event.data)
                    case EventType.VOTE:
                        self.vote(event.data)
                    case EventType.APPEND_ENTRIES:
                        self.append_entries(event.data)
                    case EventType.COMMIT_ENTRIES:
                        self.commit_entries(event.data)
                    case EventType.TIMEOUT:
                        self.heartbeat_timer.stop()
                        return CandidateState(self.raft_node)
        return self
    
    def heartbeat (self, event_data):
        if event_data.term >= self.raft_node.term:
            self.heartbeat_timer.reset()
            if event_data.leader != self.raft_node.leader or event_data.term != self.raft_node.term:
                self.raft_node.leader = event_data.leader
                self.raft_node.term = event_data.term
    
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
    
    def timeout_event (self):
        print(f"{self.raft_node.name} timed out")
        event = {
            "type": EventType.TIMEOUT,
            "messageType": MessageType.EVENT
        }
        self.proxy_call(self.raft_node.uri, event)
    
    def append_entries(self, event_data):
        print(f"{self.raft_node.name} received append entries from leader {event_data.leader}")                            
        if len(self.raft_node.log) == 0:
            previous_log = None
        else:
            previous_log = self.raft_node.log[-1]
        if event_data.previous_log != previous_log:
            print(f"{self.raft_node.name} log is inconsistent with leader's log")
            status = "inconsistent"
        else:
            self.append_waiting[event_data.log.id] = event_data.log
            status = "consistent"
        reply = {
            "type": EventType.APPEND_ENTRIES,
            "messageType": MessageType.REPLY,
            "data": {
                "node": self.raft_node.name,
                "uri": str(self.raft_node.uri),
                "status": status,
                "logId": event_data.log.id
            }
        }
        self.proxy_call(str(event_data.uri), reply)
    
    def commit_entries(self, event_data):
        print(f"{self.raft_node.name} committing entries")
        self.raft_node.log.append(self.append_waiting[event_data.logId])
        self.append_waiting.pop(event_data.logId)
        print(f"{self.raft_node.name} log is now {self.raft_node.log}")
