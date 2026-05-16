import socket
import threading
import json
import struct

class NodeServer:
    def __init__(self, ip, port, core_node):
        self.ip = ip
        self.port = int(port)
        self.core_node = core_node
        self.server_socket = None
        self.is_running = True

    def start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_socket.bind((self.ip, self.port))
            self.server_socket.listen(5)
            print(f" [SERVER] Listening on {self.ip}:{self.port}")
            
            while self.is_running:
                client_sock, client_addr = self.server_socket.accept()
                print(f" [SERVER] New connection from {client_addr}")
                
                self.core_node.connected_clients.append({
                    "socket": client_sock,
                    "address": client_addr,
                    "callback_port": None
                })

                t = threading.Thread(target=self.handle_client, args=(client_sock,))
                t.daemon = True
                t.start()
        except Exception as e:
            if self.is_running:
                print(f" [SERVER] Error: {e}")

    def handle_client(self, client_sock):
        while self.is_running:
            try:
                # Citim lungimea payload-ului (4 bytes binar)
                header = client_sock.recv(4)
                if not header or len(header) < 4:
                    break
                
                payload_len = struct.unpack("!I", header)[0]
                
                # Citim fragmentul complet de date
                data = bytearray()
                while len(data) < payload_len:
                    packet = client_sock.recv(payload_len - len(data))
                    if not packet:
                        break
                    data.extend(packet)
                
                if len(data) < payload_len:
                    break
                
                message = json.loads(data.decode('utf-8'))
                self.core_node.handle_message(message, client_sock)
            except (ConnectionResetError, ConnectionAbortedError):
                break
            except Exception as e:
                print(f"[SERVER] Error handling client: {e}")
                break
        
        print(f"[SERVER] Client disconnected.")
        self.core_node.remove_client(client_sock)
        client_sock.close()

    def stop(self):
        self.is_running = False
        if self.server_socket:
            self.server_socket.close()