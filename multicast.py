import socket
import struct
import traceback

class MulticastService:
    def __init__(self, message_update_callback):
        self.message_update_callback = message_update_callback
    
    def send_multicast(self, service_info, multicast_group='224.1.1.1', port=50000):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ttl = struct.pack('b', 1)  # Time-to-live of the message
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)

        try:
            message = str(service_info).encode()
            sock.sendto(message, (multicast_group, port))
        except Exception:
            print(traceback.format_exc())
        finally:
            sock.close()
            
    def receive_multicast(self, multicast_group='224.1.1.1', port=50000):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', port))

        group = socket.inet_aton(multicast_group)
        mreq = struct.pack('4sL', group, socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        try:
            while True:
                data, address = sock.recvfrom(1024)
                message = eval(data.decode())
                self.message_update_callback(message)
        finally:
            sock.close()