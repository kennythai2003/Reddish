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
#include <chrono>
#include <algorithm>

int main(int argc, char **argv) {
  std::unordered_map<std::string, std::string> kv_store;
  // Store expiry as milliseconds since epoch
  std::unordered_map<std::string, std::chrono::steady_clock::time_point> expiry_store;
  // Store lists for RPUSH
  std::unordered_map<std::string, std::vector<std::string>> list_store;
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
            // Handle RPUSH (create new list with single element)
            if (request.find("RPUSH") != std::string::npos) {
              // Parse RESP array for RPUSH: *N\r\n$5\r\nRPUSH\r\n$<klen>\r\n<key>\r\n$<elen1>\r\n<elem1>\r\n[$<elen2>\r\n<elem2>\r\n ...]
              size_t rpush_pos = request.find("RPUSH");
              // Find the first $ after RPUSH (key)
              size_t key_dollar = request.find('$', rpush_pos);
              if (key_dollar != std::string::npos) {
                size_t key_len_end = request.find("\r\n", key_dollar);
                if (key_len_end != std::string::npos) {
                  int key_len = std::stoi(request.substr(key_dollar + 1, key_len_end - key_dollar - 1));
                  size_t key_start = key_len_end + 2;
                  std::string key = request.substr(key_start, key_len);
                  size_t cursor = key_start + key_len;
                  std::vector<std::string> elements;
                  // Parse all elements after the key
                  while (true) {
                    size_t elem_dollar = request.find('$', cursor);
                    if (elem_dollar == std::string::npos) break;
                    size_t elem_len_end = request.find("\r\n", elem_dollar);
                    if (elem_len_end == std::string::npos) break;
                    int elem_len = std::stoi(request.substr(elem_dollar + 1, elem_len_end - elem_dollar - 1));
                    size_t elem_start = elem_len_end + 2;
                    if (elem_start + elem_len > request.size()) break;
                    std::string elem = request.substr(elem_start, elem_len);
                    elements.push_back(elem);
                    cursor = elem_start + elem_len;
                  }
                  // Insert elements into the list
                  if (elements.size() > 0) {
                    if (list_store.find(key) == list_store.end()) {
                      list_store[key] = elements;
                    } else {
                      list_store[key].insert(list_store[key].end(), elements.begin(), elements.end());
                    }
                    std::string response = ":" + std::to_string(list_store[key].size()) + "\r\n";
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
  }
  // Cleanup: close all fds
  for (int fd = 0; fd <= fd_max; ++fd) {
    if (FD_ISSET(fd, &master_set)) close(fd);
  }
  return 0;
}