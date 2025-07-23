#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unordered_map>

int main(int argc, char **argv) {
  std::unordered_map<std::string, std::string> kv_store;
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
   std::cerr << "Failed to create server socket\n";
   return 1;
  }
  
  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }
  
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);
  
  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }
  
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }
  
  struct sockaddr_in client_addr;
  int client_addr_len = sizeof(client_addr);
  std::cout << "Waiting for a client to connect...\n";

  // You can use print statements as follows for debugging, they'll be visible when running tests.
  std::cout << "Logs from your program will appear here!\n";

  fd_set master_set, read_fds;
  int fd_max = server_fd;
  FD_ZERO(&master_set);
  FD_SET(server_fd, &master_set);

  std::cout << "Server event loop started. Waiting for clients...\n";

  while (true) {
    read_fds = master_set;
    int activity = select(fd_max + 1, &read_fds, NULL, NULL, NULL);
    if (activity < 0) {
      std::cerr << "select() failed\n";
      break;
    }

    for (int fd = 0; fd <= fd_max; ++fd) {
      if (FD_ISSET(fd, &read_fds)) {
        if (fd == server_fd) {
          // New client connection
          int new_client_fd = accept(server_fd, (struct sockaddr *)&client_addr, (socklen_t *)&client_addr_len);
          if (new_client_fd < 0) {
            std::cerr << "Failed to accept client connection\n";
            continue;
          }
          FD_SET(new_client_fd, &master_set);
          if (new_client_fd > fd_max) fd_max = new_client_fd;
          std::cout << "Client connected: fd=" << new_client_fd << "\n";
        } else {
          // Data from existing client
          char buffer[1024] = {0};
          int bytes_read = read(fd, buffer, sizeof(buffer));
          if (bytes_read <= 0) {
            if (bytes_read < 0) std::cerr << "failed to read from client fd=" << fd << "\n";
            else std::cout << "Client disconnected: fd=" << fd << "\n";
            close(fd);
            FD_CLR(fd, &master_set);
          } else {
            std::string request(buffer, bytes_read);
            // Handle PING
            if (request.find("PING") != std::string::npos) {
              std::string respond("+PONG\r\n");
              if (write(fd, respond.c_str(), respond.size()) < 0) {
                std::cerr << "Failed to send response to client fd=" << fd << "\n";
                close(fd);
                FD_CLR(fd, &master_set);
              }
            }
            // Handle ECHO
            else if (request.find("ECHO") != std::string::npos) {
              // Parse RESP array: *2\r\n$4\r\nECHO\r\n$<len>\r\n<arg>\r\n
              // Find the position of the argument
              size_t echo_pos = request.find("ECHO");
              size_t arg_start = request.find("\r\n", echo_pos);
              if (arg_start != std::string::npos) {
                // Find next $ (start of argument bulk string)
                size_t dollar_pos = request.find('$', arg_start);
                if (dollar_pos != std::string::npos) {
                  size_t len_end = request.find("\r\n", dollar_pos);
                  if (len_end != std::string::npos) {
                    int arg_len = std::stoi(request.substr(dollar_pos + 1, len_end - dollar_pos - 1));
                    size_t arg_val_start = len_end + 2;
                    std::string arg = request.substr(arg_val_start, arg_len);
                    // RESP bulk string: $<len>\r\n<arg>\r\n
                    std::string response = "$" + std::to_string(arg.length()) + "\r\n" + arg + "\r\n";
                    if (write(fd, response.c_str(), response.size()) < 0) {
                      std::cerr << "Failed to send response to client fd=" << fd << "\n";
                      close(fd);
                      FD_CLR(fd, &master_set);
                    }
                  }
                }
              }
            }
            // Handle SET
            else if (request.find("SET") != std::string::npos) {
              // Parse RESP array: *3\r\n$3\r\nSET\r\n$<klen>\r\n<key>\r\n$<vlen>\r\n<val>\r\n
              // Find SET
              size_t set_pos = request.find("SET");
              size_t key_dollar = request.find('$', set_pos);
              if (key_dollar != std::string::npos) {
                size_t key_len_end = request.find("\r\n", key_dollar);
                if (key_len_end != std::string::npos) {
                  int key_len = std::stoi(request.substr(key_dollar + 1, key_len_end - key_dollar - 1));
                  size_t key_start = key_len_end + 2;
                  std::string key = request.substr(key_start, key_len);
                  // Find value
                  size_t val_dollar = request.find('$', key_start + key_len);
                  if (val_dollar != std::string::npos) {
                    size_t val_len_end = request.find("\r\n", val_dollar);
                    if (val_len_end != std::string::npos) {
                      int val_len = std::stoi(request.substr(val_dollar + 1, val_len_end - val_dollar - 1));
                      size_t val_start = val_len_end + 2;
                      std::string val = request.substr(val_start, val_len);
                      kv_store[key] = val;
                      std::string response = "+OK\r\n";
                      if (write(fd, response.c_str(), response.size()) < 0) {
                        std::cerr << "Failed to send response to client fd=" << fd << "\n";
                        close(fd);
                        FD_CLR(fd, &master_set);
                      }
                    }
                  }
                }
              }
            }
            // Handle GET
            else if (request.find("GET") != std::string::npos) {
              // Parse RESP array: *2\r\n$3\r\nGET\r\n$<klen>\r\n<key>\r\n
              size_t get_pos = request.find("GET");
              size_t key_dollar = request.find('$', get_pos);
              if (key_dollar != std::string::npos) {
                size_t key_len_end = request.find("\r\n", key_dollar);
                if (key_len_end != std::string::npos) {
                  int key_len = std::stoi(request.substr(key_dollar + 1, key_len_end - key_dollar - 1));
                  size_t key_start = key_len_end + 2;
                  std::string key = request.substr(key_start, key_len);
                  auto it = kv_store.find(key);
                  std::string response;
                  if (it != kv_store.end()) {
                    response = "$" + std::to_string(it->second.length()) + "\r\n" + it->second + "\r\n";
                  } else {
                    response = "$-1\r\n"; // Null bulk string
                  }
                  if (write(fd, response.c_str(), response.size()) < 0) {
                    std::cerr << "Failed to send response to client fd=" << fd << "\n";
                    close(fd);
                    FD_CLR(fd, &master_set);
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  // Cleanup: close all fds
  for (int fd = 0; fd <= fd_max; ++fd) {
    if (FD_ISSET(fd, &master_set)) close(fd);
  }
  return 0;
}
