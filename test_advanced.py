#!/usr/bin/env python3
"""
Advanced functionality tests for Reddish Redis server
"""
import socket
import time
import subprocess
import sys
import os
import threading

class RedisClient:
    def __init__(self, host='localhost', port=6379):
        self.host = host
        self.port = port
        self.socket = None
    
    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
    
    def disconnect(self):
        if self.socket:
            self.socket.close()
            self.socket = None
    
    def send_command(self, *args):
        """Send a Redis command using RESP protocol"""
        command = f"*{len(args)}\r\n"
        for arg in args:
            arg_str = str(arg)
            command += f"${len(arg_str)}\r\n{arg_str}\r\n"
        
        self.socket.send(command.encode())
        return self.read_response()
    
    def read_response(self):
        """Read and parse RESP response"""
        response = b""
        while True:
            chunk = self.socket.recv(1024)
            if not chunk:
                break
            response += chunk
            if response.endswith(b'\r\n'):
                break
        
        return response.decode().strip()

def start_server():
    """Start the Redis server"""
    server_path = os.path.join('build', 'server')
    if not os.path.exists(server_path):
        print(f"❌ Server executable not found at {server_path}")
        return None
    
    process = subprocess.Popen([server_path], 
                              stdout=subprocess.PIPE, 
                              stderr=subprocess.PIPE)
    
    # Give server time to start
    time.sleep(2)
    
    return process

def test_set_with_expiry():
    """Test SET command with PX (milliseconds) expiry"""
    print("🔍 Testing SET with expiry...")
    client = RedisClient()
    try:
        client.connect()
        
        # Set key with 1000ms (1 second) expiry
        response = client.send_command("SET", "expirekey", "expirevalue", "PX", "1000")
        if "+OK" not in response:
            print(f"❌ SET with expiry failed. Expected '+OK', got: {response}")
            return False
        
        # Check key exists immediately
        response = client.send_command("GET", "expirekey")
        if "expirevalue" not in response:
            print(f"❌ GET immediately after SET with expiry failed. Expected 'expirevalue', got: {response}")
            return False
        
        # Wait for expiry and check key is gone
        time.sleep(1.5)
        response = client.send_command("GET", "expirekey")
        if "$-1" in response or "(nil)" in response:
            print("✅ SET with expiry test passed")
            return True
        else:
            print(f"❌ Key did not expire. Expected '$-1' or '(nil)', got: {response}")
            return False
            
    except Exception as e:
        print(f"❌ SET with expiry test failed with error: {e}")
        return False
    finally:
        client.disconnect()

def test_multiple_clients():
    """Test multiple clients can connect simultaneously"""
    print("🔍 Testing multiple client connections...")
    
    clients = []
    try:
        # Create multiple clients
        for i in range(3):
            client = RedisClient()
            client.connect()
            clients.append(client)
        
        # Each client sets and gets a unique key
        for i, client in enumerate(clients):
            key = f"client{i}key"
            value = f"client{i}value"
            
            response = client.send_command("SET", key, value)
            if "+OK" not in response:
                print(f"❌ Client {i} SET failed")
                return False
            
            response = client.send_command("GET", key)
            if value not in response:
                print(f"❌ Client {i} GET failed")
                return False
        
        print("✅ Multiple clients test passed")
        return True
        
    except Exception as e:
        print(f"❌ Multiple clients test failed with error: {e}")
        return False
    finally:
        for client in clients:
            client.disconnect()

def test_keys_command():
    """Test KEYS command"""
    print("🔍 Testing KEYS command...")
    client = RedisClient()
    try:
        client.connect()
        
        # Set some test keys
        test_keys = ["key1", "key2", "testkey"]
        for key in test_keys:
            client.send_command("SET", key, f"value_for_{key}")
        
        # Get all keys
        response = client.send_command("KEYS", "*")
        
        # Check that our keys are present
        keys_found = 0
        for key in test_keys:
            if key in response:
                keys_found += 1
        
        if keys_found >= len(test_keys):
            print("✅ KEYS command test passed")
            return True
        else:
            print(f"❌ KEYS command test failed. Expected {len(test_keys)} keys, found {keys_found}")
            print(f"Response: {response}")
            return False
            
    except Exception as e:
        print(f"❌ KEYS command test failed with error: {e}")
        return False
    finally:
        client.disconnect()

def test_config_get():
    """Test CONFIG GET command"""
    print("🔍 Testing CONFIG GET command...")
    client = RedisClient()
    try:
        client.connect()
        
        # Test CONFIG GET dir
        response = client.send_command("CONFIG", "GET", "dir")
        if "*2" in response or "dir" in response:
            print("✅ CONFIG GET test passed")
            return True
        else:
            print(f"❌ CONFIG GET test failed. Response: {response}")
            return False
            
    except Exception as e:
        print(f"❌ CONFIG GET test failed with error: {e}")
        return False
    finally:
        client.disconnect()

def run_tests():
    """Run all advanced tests"""
    print("🚀 Starting Reddish Redis Server Advanced Tests")
    print("=" * 50)
    
    # Start server
    server_process = start_server()
    if not server_process:
        return False
    
    try:
        # Check if server is running
        if server_process.poll() is not None:
            print("❌ Server failed to start")
            return False
        
        # Run tests
        tests = [
            test_set_with_expiry,
            test_multiple_clients,
            test_keys_command,
            test_config_get
        ]
        
        passed = 0
        total = len(tests)
        
        for test in tests:
            if test():
                passed += 1
            print()  # Empty line between tests
        
        print("=" * 50)
        print(f"🎯 Results: {passed}/{total} tests passed")
        
        if passed == total:
            print("🎉 All advanced tests passed!")
            return True
        else:
            print("💥 Some advanced tests failed!")
            return False
            
    finally:
        # Clean up server
        if server_process:
            server_process.terminate()
            server_process.wait()

if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)