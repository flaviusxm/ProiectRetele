import socket
import threading
import json
import struct

class NodeClient:
    def __init__(self, bootstrap_servers, core_node):
        self.bootstrap_servers = bootstrap_servers
        self.core_node = core_node
        self.upstream_socket = None
        self.is_running = True

    def connect_to_network(self):
        print("[CLIENT] Connecting ...")
        
        for server_addr in self.bootstrap_servers:
            if server_addr == (self.core_node.my_ip, self.core_node.my_port):
                continue
            
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(server_addr)
                self.upstream_socket = sock
                print(f"[CLIENT] Connected to server: {server_addr}")
                
                self.send_to_upstream({
                    "type": "HANDSHAKE",
                    "payload": {
                        "ip": self.core_node.my_ip,
                        "port": self.core_node.my_port
                    }
                })

                t = threading.Thread(target=self.listen_to_upstream)
                t.daemon = True
                t.start()
                return True
            except ConnectionRefusedError:
                print(f"[CLIENT] Server {server_addr} not responding.")
                continue
        
        print("[CLIENT] No server available.")
        return False

    def listen_to_upstream(self):
        while self.is_running and self.upstream_socket:
            try:
                header = self.upstream_socket.recv(4)
                if not header or len(header) < 4:
                    print("[CLIENT] Server closed connection (empty header).")
                    break
                
                payload_len = struct.unpack("!I", header)[0]
                
                data = bytearray()
                while len(data) < payload_len:
                    packet = self.upstream_socket.recv(payload_len - len(data))
                    if not packet:
                        break
                    data.extend(packet)
                
                if len(data) < payload_len:
                    break
                
                message = json.loads(data.decode('utf-8'))
                self.core_node.handle_message(message, self.upstream_socket)
            except Exception as e:
                print(f"[CLIENT] Receive error: {e}")
                break
        self.upstream_socket = None

    def send_to_upstream(self, message_dict):
        if self.upstream_socket:
            try:
                data_bytes = json.dumps(message_dict).encode('utf-8')
                header = struct.pack("!I", len(data_bytes))
                self.upstream_socket.sendall(header + data_bytes)
            except Exception as e:
                print(f"[CLIENT] Send error: {e}")