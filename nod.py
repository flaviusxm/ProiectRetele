import sys
import threading
import time
import json
import socket
import struct
import subprocess
from server import NodeServer
from client import NodeClient

class Node:
    def __init__(self, my_ip, my_port, bootstrap_servers):
        self.my_ip = my_ip
        self.my_port = int(my_port)
        self.connected_clients = [] 
        self.subscriptions = {}      
        
        self.MAX_PAYLOAD_SIZE = 1024 * 1024 
        self.commands = {
            "uppercase": [sys.executable, "-c", "import sys; print(sys.stdin.read().upper().strip())"],
            "count_words": [sys.executable, "-c", "import sys; print(f'Numar cuvinte: {len(sys.stdin.read().split())}')"]
        }
        
        self.server_module = NodeServer(self.my_ip, self.my_port, self)
        self.client_module = NodeClient(bootstrap_servers, self)

    def run(self):
        server_thread = threading.Thread(target=self.server_module.start)
        server_thread.daemon = True
        server_thread.start()
        
        time.sleep(0.5)
        self.client_module.connect_to_network()

        try:
            while True:
                cmd_line = input(f"\n[{self.my_ip}:{self.my_port}] > ").strip()
                if not cmd_line: continue
                
                parts = cmd_line.split(" ", 2)
                cmd = parts[0].lower()

                if cmd == "exit":
                    break
                elif cmd == "status":
                    self.show_status()
                elif cmd == "sub" and len(parts) > 1:
                    self.subscribe_locally(parts[1])
                elif cmd == "unsub" and len(parts) > 1:
                    self.unsubscribe_locally(parts[1])
                elif cmd == "pub" and len(parts) > 2:
                    self.publish_message(parts[1], parts[2])
                else:
                    print(" Comenzi valide: status, sub <cheie>, unsub <cheie>, pub <cheie> <mesaj_sau_job>, exit")
                    print(" Chei cu procesare configurata: 'uppercase', 'count_words'")
        except KeyboardInterrupt:
            pass
        finally:
            self.server_module.stop()
            print(" Node stopped successfully.")

    def show_status(self):
        print(f"\n--- NODE STATUS ({self.my_ip}:{self.my_port}) ---")
        print(f" Connected to upstream: {self.client_module.upstream_socket is not None}")
        print(f" Directly connected clients: {len(self.connected_clients)}")
        for c in self.connected_clients:
            print(f"   - {c['address']} (Callback Port: {c['callback_port']})")
        clean_subs = {k: list(v) for k, v in self.subscriptions.items()}
        print(f" Active subscriptions: {clean_subs}")

    def handle_message(self, message, sender_sock):
        m_type = message.get("type")
        payload = message.get("payload", {})

        if m_type == "HANDSHAKE":
            for c in self.connected_clients:
                if c["socket"] == sender_sock:
                    c["callback_port"] = payload.get("port")
                    print(f"\n [INFO] Handshake received from {payload.get('ip')}:{payload.get('port')}")
                    
                    announcement = {
                        "type": "PEER_ANNOUNCEMENT",
                        "payload": {"ip": payload.get("ip"), "port": payload.get("port")}
                    }
                    self.propagate_message(announcement, sender_sock)
                    break

        elif m_type == "PEER_ANNOUNCEMENT":
            self.propagate_message(message, sender_sock)

        elif m_type == "SUBSCRIBE":
            key = payload.get("key")
            subscriber = tuple(payload.get("subscriber"))
            if key not in self.subscriptions:
                self.subscriptions[key] = set()
            
            if subscriber not in self.subscriptions[key]:
                self.subscriptions[key].add(subscriber)
                print(f"\n [INFO] New subscription: {subscriber} on key '{key}'")
                self.propagate_message(message, sender_sock)

        elif m_type == "UNSUBSCRIBE":
            key = payload.get("key")
            subscriber = tuple(payload.get("subscriber"))
            if key in self.subscriptions and subscriber in self.subscriptions[key]:
                self.subscriptions[key].remove(subscriber)
                print(f"\n [INFO] Unsubscription: {subscriber} from key '{key}'")
                self.propagate_message(message, sender_sock)

        elif m_type == "MESSAGE":
            key = payload.get("key")
            data = payload.get("data")
            print(f"\n [MSG] Received binary job on key '{key}' (Length: {len(data)} bytes)")
            if key in self.subscriptions and (self.my_ip, self.my_port) in self.subscriptions[key]:
                self.execute_command(key, data)
                
            self.distribute_message(key, data, message, sender_sock)

    def subscribe_locally(self, key):
        subscriber = (self.my_ip, self.my_port)
        if key not in self.subscriptions:
            self.subscriptions[key] = set()
        self.subscriptions[key].add(subscriber)
        
        msg = {
            "type": "SUBSCRIBE",
            "payload": {"key": key, "subscriber": subscriber}
        }
        self.broadcast_message(msg)
        print(f" Subscribed to key '{key}'")

    def unsubscribe_locally(self, key):
        subscriber = (self.my_ip, self.my_port)
        if key in self.subscriptions and subscriber in self.subscriptions[key]:
            self.subscriptions[key].remove(subscriber)
            msg = {
                "type": "UNSUBSCRIBE",
                "payload": {"key": key, "subscriber": subscriber}
            }
            self.broadcast_message(msg)
            print(f" Unsubscribed from key '{key}'")

    def publish_message(self, key, data):
        msg = {
            "type": "MESSAGE",
            "payload": {"key": key, "data": data}
        }
        print(f" Published binary job on key '{key}'")
        if key in self.subscriptions and (self.my_ip, self.my_port) in self.subscriptions[key]:
            self.execute_command(key, data)
        
        self.distribute_message(key, data, msg, None)

    def execute_command(self, key, data):
        """Execută o comandă reală în proces independent (Cerința 2.6)"""
        print(f" >>> [EXEC] Routing payload to terminal process for key '{key}'...")
        if key in self.commands:
            try:
                proc = subprocess.Popen(
                    self.commands[key],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                stdout, stderr = proc.communicate(input=str(data), timeout=5.0)
                
                print(f" ------------ REZULTAT PROCESARE REALĂ ------------")
                if stdout: print(f"{stdout.strip()}")
                if stderr: print(f"Eroare proces: {stderr.strip()}")
                print(f" --------------------------------------------------")
            except subprocess.TimeoutExpired:
                print("[!] Timeout la executarea comenzii secundare.")
            except Exception as e:
                print(f"[!] Eroare la execuția subprocess: {e}")
        else:
            print(f" No real OS command mapped for key '{key}' (Simulated fallback: {data[::-1]})")

    def broadcast_message(self, msg):
        self.client_module.send_to_upstream(msg)
        for c in self.connected_clients:
            self.send_safe_binary(c["socket"], msg)

    def propagate_message(self, msg, exclude_sock):
        if self.client_module.upstream_socket != exclude_sock:
            self.client_module.send_to_upstream(msg)
        
        for c in self.connected_clients:
            if c["socket"] != exclude_sock:
                self.send_safe_binary(c["socket"], msg)

    def distribute_message(self, key, data, original_msg, sender_sock):
        if key in self.subscriptions:
            for sub_ip, sub_port in self.subscriptions[key]:
                if (sub_ip, sub_port) == (self.my_ip, self.my_port):
                    continue
                threading.Thread(target=self.direct_delivery, args=(sub_ip, sub_port, original_msg), daemon=True).start()

    def direct_delivery(self, ip, port, msg):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(3.0)
                s.connect((ip, port))
                self.send_safe_binary(s, msg)
        except Exception:
            pass

    def send_safe_binary(self, sock, msg_dict):
        """Sistem anti-flood și fragmentare binară: [Header 4 Bytes - Lungime][Payload JSON]"""
        try:
            data_bytes = json.dumps(msg_dict).encode('utf-8')
            if len(data_bytes) > self.MAX_PAYLOAD_SIZE:
                print("[!] Payload blocked: Size exceeds flood control limits!")
                return
            header = struct.pack("!I", len(data_bytes))
            sock.sendall(header + data_bytes)
        except Exception:
            pass

    def remove_client(self, sock):
        for i, c in enumerate(self.connected_clients):
            if c["socket"] == sock:
                port = c["callback_port"]
                del self.connected_clients[i]
                if port:
                    print(f" [CLEANUP] Removing disconnected consumer on callback port {port} from all keys")
                    for key in self.subscriptions:
                        self.subscriptions[key] = {s for s in self.subscriptions[key] if s[1] != port}
                break

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python nod.py <LOCAL_IP> <LOCAL_PORT> <SERVER1_IP:PORT,SERVER2_IP:PORT...>")
        sys.exit(1)

    ip_local = sys.argv[1]
    port_local = sys.argv[2]
    
    bootstrap = []
    if len(sys.argv) > 3:
        raw_servers = sys.argv[3].split(",")
        for s in raw_servers:
            if ":" in s:
                s_ip, s_port = s.split(":")
                bootstrap.append((s_ip, int(s_port)))

    nod = Node(ip_local, port_local, bootstrap)
    nod.run()