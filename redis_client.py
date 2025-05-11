import socket
import sys

class RedisClient:
    def __init__(self, host='127.0.0.1', port=6379):
        self.host = host
        self.port = port
        self.socket = None
    
    def connect(self):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            print(f"Connected to Redis at {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"Error connecting to Redis: {e}")
            return False
    
    def disconnect(self):
        if self.socket:
            self.socket.close()
            self.socket = None
            print("Disconnected from Redis")
    
    def execute_command(self, *args):
        if not self.socket:
            if not self.connect():
                return None
        
        command = f"*{len(args)}\r\n"
        for arg in args:
            arg_str = str(arg)
            command += f"${len(arg_str)}\r\n{arg_str}\r\n"
        
        try:
            self.socket.sendall(command.encode())
            response = self.socket.recv(4096).decode('utf-8')
            return self._parse_response(response)
        except Exception as e:
            print(f"Error executing command: {e}")
            self.disconnect()
            return None
    
    def _parse_response(self, response):
        if not response:
            return None
        
        first_char = response[0]
        
        if first_char == '+': 
            return response[1:].strip()
        elif first_char == '-':
            return f"ERROR: {response[1:].strip()}"
        elif first_char == ':':
            return int(response[1:].strip())
        elif first_char == '$':
            if response[1:3] == '-1':
                return None 
            parts = response.split('\r\n')
            if len(parts) >= 3:
                return parts[1]
        
        return response.strip()

def interactive_client():
    client = RedisClient()
    
    if not client.connect():
        sys.exit(1)
    
    print("Redis Client Connected. Type commands or 'exit' to quit.")
    print("Examples: PING, SET key value, GET key, DEL key")
    
    try:
        while True:
            cmd_line = input("redis> ")
            if cmd_line.lower() in ('exit', 'quit'):
                break
            
            args = cmd_line.strip().split()
            if not args:
                continue
            
            result = client.execute_command(*args)
            print(result)
    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        client.disconnect()

if __name__ == "__main__":
    interactive_client()