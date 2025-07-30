#!/usr/bin/env python3
"""
Basic functionality tests for Reddish Redis server
"""
import socket
import time
import subprocess
import sys
import os
import signal
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

def test_ping():
    """Test PING command"""
    print("🔍 Testing PING command...")
    client = RedisClient()
    try:
        client.connect()
        response = client.send_command("PING")
        if "+PONG" in response:
            print("✅ PING test passed")
            return True
        else:
            print(f"❌ PING test failed. Expected '+PONG', got: {response}")
            return False
    except Exception as e:
        print(f"❌ PING test failed with error: {e}")
        return False
    finally:
        client.disconnect()

def test_set_get():
    """Test SET and GET commands"""
    print("🔍 Testing SET/GET commands...")
    client = RedisClient()
    try:
        client.connect()
        
        # Test SET
        response = client.send_command("SET", "testkey", "testvalue")
        if "+OK" not in response:
            print(f"❌ SET test failed. Expected '+OK', got: {response}")
            return False
        
        # Test GET
        response = client.send_command("GET", "testkey")
        if "testvalue" not in response:
            print(f"❌ GET test failed. Expected 'testvalue', got: {response}")
            return False
        
        print("✅ SET/GET test passed")
        return True
    except Exception as e:
        print(f"❌ SET/GET test failed with error: {e}")
        return False
    finally:
        client.disconnect()

def test_nonexistent_key():
    """Test GET on non-existent key"""
    print("🔍 Testing GET on non-existent key...")
    client = RedisClient()
    try:
        client.connect()
        response = client.send_command("GET", "nonexistent")
        if "$-1" in response or "(nil)" in response:
            print("✅ Non-existent key test passed")
            return True
        else:
            print(f"❌ Non-existent key test failed. Expected '$-1' or '(nil)', got: {response}")
            return False
    except Exception as e:
        print(f"❌ Non-existent key test failed with error: {e}")
        return False
    finally:
        client.disconnect()

def run_tests():
    """Run all tests"""
    print("🚀 Starting Reddish Redis Server Tests")
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
            test_ping,
            test_set_get,
            test_nonexistent_key
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
            print("🎉 All tests passed!")
            return True
        else:
            print("💥 Some tests failed!")
            return False
            
    finally:
        # Clean up server
        if server_process:
            server_process.terminate()
            server_process.wait()

if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)