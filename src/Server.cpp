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
#include <vector>
#include <tuple>
#include "RespParser.h"
#include "CommandHandler.h"

using Clock = std::chrono::steady_clock;
using TimePoint = std::chrono::time_point<Clock>;

struct BlpopWaiter {
    int fd;
    TimePoint start;
    double timeout; // seconds, 0 means infinite
};

int main(int argc, char **argv) {
  std::unordered_map<std::string, std::string> kv_store;
  // Store expiry as milliseconds since epoch
  std::unordered_map<std::string, std::chrono::steady_clock::time_point> expiry_store;
  // Store lists for RPUSH
  std::unordered_map<std::string, std::vector<std::string>> list_store;
  // For each list, a queue of waiting clients (fd, start, timeout)
  std::unordered_map<std::string, std::queue<BlpopWaiter>> blpop_waiting_clients;
  // For each client fd, the list they are waiting for
  std::unordered_map<int, std::string> client_waiting_for_list;
  // For each client fd, their timeout and start time
  std::unordered_map<int, std::pair<TimePoint, double>> client_waiting_time;
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
    // Compute select timeout for BLPOP
    timeval *timeout_ptr = nullptr;
    timeval timeout_val;
    double min_timeout = -1;
    TimePoint now = Clock::now();
    for (const auto& [list, queue] : blpop_waiting_clients) {
      std::queue<BlpopWaiter> q = queue;
      while (!q.empty()) {
        const BlpopWaiter& w = q.front();
        if (w.timeout > 0) {
          double elapsed = std::chrono::duration<double>(now - w.start).count();
          double remain = w.timeout - elapsed;
          if (remain < 0) remain = 0;
          if (min_timeout < 0 || remain < min_timeout) min_timeout = remain;
        }
        q.pop();
      }
    }
    if (min_timeout >= 0) {
      timeout_val.tv_sec = (int)min_timeout;
      timeout_val.tv_usec = (int)((min_timeout - (int)min_timeout) * 1e6);
      timeout_ptr = &timeout_val;
    }
    int activity = select(fd_max + 1, &read_fds, NULL, NULL, timeout_ptr);
    now = Clock::now();
    if (activity < 0) {
      std::cerr << "select() failed\n";
      break;
    }
    // Handle BLPOP timeouts
    for (auto& [list, queue] : blpop_waiting_clients) {
      std::queue<BlpopWaiter> new_queue;
      while (!queue.empty()) {
        BlpopWaiter w = queue.front(); queue.pop();
        if (w.timeout > 0) {
          double elapsed = std::chrono::duration<double>(now - w.start).count();
          if (elapsed >= w.timeout) {
            // Timeout expired, respond with $-1\r\n
            write(w.fd, "$-1\r\n", 5);
            client_waiting_for_list.erase(w.fd);
            client_waiting_time.erase(w.fd);
            close(w.fd);
            FD_CLR(w.fd, &master_set);
            continue;
          }
        }
        new_queue.push(w);
      }
      queue = std::move(new_queue);
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
              auto &wait_queue = blpop_waiting_clients[list_key];
              std::queue<BlpopWaiter> new_queue;
              while (!wait_queue.empty()) {
                BlpopWaiter w = wait_queue.front(); wait_queue.pop();
                if (w.fd != fd) new_queue.push(w);
              }
              wait_queue = std::move(new_queue);
              client_waiting_for_list.erase(it);
              client_waiting_time.erase(fd);
            }
            close(fd);
            FD_CLR(fd, &master_set);
          } else {
            std::string request(buffer, bytes_read);
            std::vector<std::string> args = RespParser::parseArray(request);
            // BLPOP handling
            if (!args.empty() && args[0] == "BLPOP" && args.size() == 3) {
              std::string list_key = args[1];
              double timeout = 0;
              try { timeout = std::stod(args[2]); } catch (...) { timeout = 0; }
              auto it = list_store.find(list_key);
              if (it == list_store.end() || it->second.empty()) {
                // List is empty, block this client
                BlpopWaiter w{fd, Clock::now(), timeout};
                blpop_waiting_clients[list_key].push(w);
                client_waiting_for_list[fd] = list_key;
                client_waiting_time[fd] = std::make_pair(w.start, timeout);
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
              std::queue<BlpopWaiter> new_queue;
              while (!wait_queue.empty() && !list_store[list_key].empty()) {
                BlpopWaiter w = wait_queue.front(); wait_queue.pop();
                client_waiting_for_list.erase(w.fd);
                client_waiting_time.erase(w.fd);
                std::string val = list_store[list_key].front();
                list_store[list_key].erase(list_store[list_key].begin());
                std::string resp = "*2\r\n$" + std::to_string(list_key.size()) + "\r\n" + list_key + "\r\n";
                resp += "$" + std::to_string(val.size()) + "\r\n" + val + "\r\n";
                if (write(w.fd, resp.c_str(), resp.size()) < 0) {
                  std::cerr << "Failed to send response to client fd=" << w.fd << "\n";
                  close(w.fd);
                  FD_CLR(w.fd, &master_set);
                }
              }
              wait_queue = std::move(new_queue);
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