import socket
import threading
import time 
from collections import defaultdict 
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('RedisServer')


class RedisServer:
    def __init__(self, host='127.0.0.1', port=6379):
        self.host = host 
        self.port = port 
        self.data_store = {}
        self.expiry = {}
        self.server_socket = None
        self.running = False
        self.lock = threading.RLock()

    def start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            self.running = True
            logger.info(f'Redis server started on {self.host}:{self.port}')

            expiry_thread = threading.Thread(target=self._check_expiry)
            expiry_thread.daemon = True
            expiry_thread.start()

            while self.running:
                client_socket, address = self.server_socket.accept()
                logger.info(f'Connection from {address}')

                client_thread = threading.Thread(target=self._handle_client, args=(client_socket, address))
                client_thread.daemon = True
                client_thread.start()

        except Exception as e:
            logger.error(f"Error in server: {e}")
        finally:
            self.stop()

    def stop(self):
        self.running = False
        if self.server_socket:
            self.server_socket.close()
            logger.info(f'Redis server has been closed on {self.host}:{self.port}') 

    def _check_expiry(self):
        while self.running:
            now = time.time()
            keys_to_delete = []

            with self.lock:
                for key, expiry_time in list(self.expiry.items()):
                    if expiry_time <= now:
                        keys_to_delete.append(key)
                
                for key in keys_to_delete:
                    logger.debug(f'Key expired: {key}')
                    self.data_store.pop(key, None)
                    self.expiry.pop(key, None)
            
            time.sleep(0.1)

    def _handle_client(self, client_socket, address):
        try:
            while self.running:
                data = self._read_command(client_socket)
                if not data:
                    break

                response = self._process_command(data)
                client_socket.sendall(response.encode())
        
        except Exception as e:
            logger.error(f"Error handling client {address}: {e}")
        finally:
            client_socket.close()
            logger.info(f'Connection closed with {address}')
    
    def _read_command(self, client_socket):
        data = client_socket.recv(1024).decode('utf-8')
        if not data:
            return None
        
        return self._parse_resp(data)

    def _parse_resp(self, data):
        lines = data.strip().split('\r\n')

        if lines[0].startswith('*'):
            num_elements = int(lines[0][1:])
            command = []

            line_idx = 1
            for _ in range(num_elements):
                if lines[line_idx].startswith('$'):
                    bulk_len = int(lines[line_idx][1:])
                    command.append(lines[line_idx + 1])
                    line_idx += 2
            return command

        return None

    def _process_command(self, command):
        if not command:
            return "-ERR Unknown command\r\n"

        cmd = command[0].upper()
        args = command[1:] if len(command) > 1 else []

        handlers = {
            'PING': self._handle_ping,
            'GET': self._handle_get,
            'SET': self._handle_set,
            'DEL': self._handle_del,
            'EXISTS': self._handle_exists,
            'EXPIRE': self._handle_expire,
            'TTL': self._handle_ttl,
            'INFO': self._handle_info,
        }

        handler = handlers.get(cmd)
        if handler:
            return handler(args)
        else:
            return "-ERR Unknown command\r\n"
        
    def _handle_ping(self, args):
        if args:
            return f"${len(args[0])}\r\n{args[0]}\r\n"
        return "+PONG\r\n"

    def _handle_get(self, args):
        if not args:
            return "-ERR wrong number of arguments for 'get' command\r\n"
        
        key = args[0]
        with self.lock:
            if key in self.data_store:
                value = self.data_store[key]
                return f"${len(value)}\r\n{value}\r\n"
            else:
                return "$-1\r\n"

    def _handle_set(self, args):
        if len(args) < 2:
            return "-ERR wrong number of arguments for 'set' command\r\n"
        
        key, value = args[0], args[1]
        ex = None

        i = 2
        while i < len(args):
            if args[i].upper() == 'EX' and i + 1 < len(args):
                try:
                    ex = int(args[i+1])
                    i += 2
                except ValueError:
                    return "-ERR value is not an integer or out of range\r\n"
            else:
                i += 1
            
        with self.lock:
            self.data_store[key] = value

            if ex is not None:
                self.expiry[key] = time.time() + ex 
            elif key in self.expiry:
                del self.expiry[key]
        
        return "+OK\r\n"
    

    def _handle_del(self, args):
        if not args:
            return "-ERR wrong number of arguments for 'del' command\r\n"
        
        count = 0
        with self.lock:
            for key in args:
                if key in self.data_store:
                    del self.data_store[key]
                    self.expiry.pop(key, None)
                    count += 1
        
        return f":{count}\r\n"
    
    def _handle_exists(self, args):
        if not args:
            return "-ERR wrong number of arguments for 'exists' command\r\n"
        
        count = 0
        with self.lock:
            for key in args:
                if key in self.data_store:
                    count += 1
        
        return f":{count}\r\n"
    
    def _handle_expire(self, args):
        if len(args) != 2:
            return "-ERR wrong number of arguments for 'expire' command\r\n"
        
        key, seconds = args[0], args[1]
        
        try:
            seconds = int(seconds)
        except ValueError:
            return "-ERR value is not an integer or out of range\r\n"
        
        with self.lock:
            if key in self.data_store:
                self.expiry[key] = time.time() + seconds
                return ":1\r\n"
            else:
                return ":0\r\n"
    
    def _handle_ttl(self, args):
        if len(args) != 1:
            return "-ERR wrong number of arguments for 'ttl' command\r\n"
        
        key = args[0]
        
        with self.lock:
            if key not in self.data_store:
                return ":-2\r\n" 
            
            if key not in self.expiry:
                return ":-1\r\n"  
            
            ttl = int(self.expiry[key] - time.time())
            return f":{max(0, ttl)}\r\n"
    
    def _handle_info(self, args):
        info_str = "# Server\r\nredis_version:1.0.0\r\n"
        info_str += f"tcp_port:{self.port}\r\n"
        
        with self.lock:
            info_str += f"# Keyspace\r\ndb0:keys={len(self.data_store)},expires={len(self.expiry)}\r\n"
        
        return f"${len(info_str)}\r\n{info_str}\r\n"

if __name__ == "__main__":
    server = RedisServer()
    try:
        server.start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
        server.stop()