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
#include <queue>
#include "RespParser.h"
#include "CommandHandler.h"

int main(int argc, char **argv) {
  std::unordered_map<std::string, std::string> kv_store;
  // Store expiry as milliseconds since epoch
  std::unordered_map<std::string, std::chrono::steady_clock::time_point> expiry_store;
  // Store lists for RPUSH
  std::unordered_map<std::string, std::vector<std::string>> list_store;
  // For each list, a queue of waiting client fds
  std::unordered_map<std::string, std::queue<int>> blpop_waiting_clients;
  // For each client fd, the list they are waiting for
  std::unordered_map<int, std::string> client_waiting_for_list;
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
    CommandHandler handler(kv_store, expiry_store, list_store);
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
            // Clean up BLPOP tracking if client disconnects
            auto it = client_waiting_for_list.find(fd);
            if (it != client_waiting_for_list.end()) {
              std::string list_key = it->second;
              // Remove fd from the waiting queue for this list
              auto &wait_queue = blpop_waiting_clients[list_key];
              std::queue<int> new_queue;
              while (!wait_queue.empty()) {
                int cur_fd = wait_queue.front(); wait_queue.pop();
                if (cur_fd != fd) new_queue.push(cur_fd);
              }
              wait_queue = std::move(new_queue);
              client_waiting_for_list.erase(it);
            }
            close(fd);
            FD_CLR(fd, &master_set);
          } else {
            std::string request(buffer, bytes_read);
            std::vector<std::string> args = RespParser::parseArray(request);
            // BLPOP handling
            if (!args.empty() && args[0] == "BLPOP" && args.size() == 3 && args[2] == "0") {
              std::string list_key = args[1];
              auto it = list_store.find(list_key);
              if (it == list_store.end() || it->second.empty()) {
                // List is empty, block this client
                blpop_waiting_clients[list_key].push(fd);
                client_waiting_for_list[fd] = list_key;
                // Do NOT respond yet
                continue;
              } else {
                // List has elements, pop and respond immediately
                std::string val = it->second.front();
                it->second.erase(it->second.begin());
                std::string resp = "*2\r\n$" + std::to_string(list_key.size()) + "\r\n" + list_key + "\r\n";
                resp += "$" + std::to_string(val.size()) + "\r\n" + val + "\r\n";
                if (write(fd, resp.c_str(), resp.size()) < 0) {
                  std::cerr << "Failed to send response to client fd=" << fd << "\n";
                  close(fd);
                  FD_CLR(fd, &master_set);
                }
                continue;
              }
            }
            // Normal command handling
            std::string response = handler.handle(args);
            // After RPUSH/LPUSH, check for blocked BLPOP clients
            if (!args.empty() && (args[0] == "RPUSH" || args[0] == "LPUSH") && args.size() >= 3) {
              std::string list_key = args[1];
              auto &wait_queue = blpop_waiting_clients[list_key];
              while (!wait_queue.empty() && !list_store[list_key].empty()) {
                int waiting_fd = wait_queue.front();
                wait_queue.pop();
                client_waiting_for_list.erase(waiting_fd);
                std::string val = list_store[list_key].front();
                list_store[list_key].erase(list_store[list_key].begin());
                std::string resp = "*2\r\n$" + std::to_string(list_key.size()) + "\r\n" + list_key + "\r\n";
                resp += "$" + std::to_string(val.size()) + "\r\n" + val + "\r\n";
                if (write(waiting_fd, resp.c_str(), resp.size()) < 0) {
                  std::cerr << "Failed to send response to client fd=" << waiting_fd << "\n";
                  close(waiting_fd);
                  FD_CLR(waiting_fd, &master_set);
                }
              }
            }
            if (!response.empty()) {
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
  // Cleanup: close all fds
  for (int fd = 0; fd <= fd_max; ++fd) {
    if (FD_ISSET(fd, &master_set)) close(fd);
  }
  return 0;
}