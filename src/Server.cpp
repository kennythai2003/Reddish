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

#include <fcntl.h>
#include <tuple>

#include "RespParser.h"
#include "CommandHandler.h"

// Allow access to global stream_store defined in CommandHandler.cpp
extern std::unordered_map<std::string, std::vector<StreamEntry>> stream_store;

using Clock = std::chrono::steady_clock;
using TimePoint = std::chrono::time_point<Clock>;

struct BlpopWaiter {
    int fd;
    TimePoint start;
    double timeout; // seconds, 0 means infinite
};

struct XreadWaiter {
    int fd;
    TimePoint start;
    double timeout; // seconds, 0 means infinite
    std::vector<std::string> stream_keys;
    std::vector<std::string> last_ids;
};

int main(int argc, char **argv) {
  // Track all connected replica fds
  std::vector<int> replica_fds;
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
  // For XREAD blocking
  std::vector<XreadWaiter> xread_waiting_clients;

  // Declare fd_set and fd_max early so they are in scope for handshake logic
  fd_set master_set, read_fds;
  int fd_max;

  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    std::cerr << "Failed to create server socket\n";
    return 1;
  }
  fd_max = server_fd;
  
  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }
  
  // Parse --port and --replicaof flags if present
  int port = 6379;
  bool is_replica = false;
  std::string master_host;
  int master_port = 0;
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "--port" && i + 1 < argc) {
      port = std::atoi(argv[i + 1]);
    }
    if (std::string(argv[i]) == "--replicaof" && i + 1 < argc) {
      is_replica = true;
      std::string arg = argv[i + 1];
      // Try to split arg into host and port
      size_t space = arg.find(' ');
      if (space != std::string::npos) {
        master_host = arg.substr(0, space);
        master_port = std::atoi(arg.substr(space + 1).c_str());
      } else {
        // fallback: try next arg as port
        master_host = arg;
        if (i + 2 < argc) master_port = std::atoi(argv[i + 2]);
      }
    }
  }
  // If replica, connect to master and send PING handshake
  int master_fd = -1;
  bool add_master_fd_to_set = false;
  if (is_replica && !master_host.empty() && master_port > 0) {
    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    std::string port_str = std::to_string(master_port);
    if (getaddrinfo(master_host.c_str(), port_str.c_str(), &hints, &res) == 0) {
      master_fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
      if (master_fd >= 0) {
        if (connect(master_fd, res->ai_addr, res->ai_addrlen) == 0) {
          // Set master_fd to blocking mode for handshake
          int flags = fcntl(master_fd, F_GETFL, 0);
          if (flags != -1) fcntl(master_fd, F_SETFL, flags & ~O_NONBLOCK);
          // Send RESP PING: *1\r\n$4\r\nPING\r\n
          std::string ping = "*1\r\n$4\r\nPING\r\n";
          ssize_t sent = send(master_fd, ping.c_str(), ping.size(), 0);
          if (sent < 0) {
            std::cerr << "Failed to send PING to master at " << master_host << ":" << master_port << "\n";
          } else {
            std::cout << "Sent PING to master at " << master_host << ":" << master_port << "\n";
            // Wait for a response from master (PONG or any RESP simple string)
            char resp_buf[128] = {0};
            ssize_t n = recv(master_fd, resp_buf, sizeof(resp_buf)-1, 0);
            if (n > 0) {
              std::string resp(resp_buf, n);
              if (!resp.empty() && resp[0] == '+') {
                // Now send REPLCONF listening-port <PORT>
                std::string port_str2 = std::to_string(port);
                std::string replconf1 = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + std::to_string(port_str2.size()) + "\r\n" + port_str2 + "\r\n";
                send(master_fd, replconf1.c_str(), replconf1.size(), 0);
                // Wait for response to REPLCONF listening-port
                char resp_buf2[128] = {0};
                ssize_t n2 = recv(master_fd, resp_buf2, sizeof(resp_buf2)-1, 0);
                if (n2 > 0) {
                  std::string resp2(resp_buf2, n2);
                  if (!resp2.empty() && resp2[0] == '+') {
                    // Now send REPLCONF capa psync2
                    std::string replconf2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
                    send(master_fd, replconf2.c_str(), replconf2.size(), 0);
                    // Wait for response to REPLCONF capa psync2
                    char resp_buf3[128] = {0};
                    ssize_t n3 = recv(master_fd, resp_buf3, sizeof(resp_buf3)-1, 0);
                    if (n3 > 0) {
                      std::string resp3(resp_buf3, n3);
                      if (!resp3.empty() && resp3[0] == '+') {
                        // Now send PSYNC ? -1
                        std::string psync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
                        send(master_fd, psync.c_str(), psync.size(), 0);

                        // Wait for FULLRESYNC response and RDB file
                        char resp_buf[1024] = {0};
                        ssize_t n = recv(master_fd, resp_buf, sizeof(resp_buf)-1, 0);
                        if (n > 0) {
                          std::string resp(resp_buf, n);
                          // Look for +FULLRESYNC response
                          if (resp.substr(0, 11) == "+FULLRESYNC") {
                            std::cout << "Received FULLRESYNC from master\n";
                            // Find the end of the +FULLRESYNC line
                            size_t line_end = resp.find("\r\n");
                            if (line_end != std::string::npos) {
                              size_t rdb_start = line_end + 2;
                              // Now we should have the RDB file starting with $<length>\r\n
                              if (rdb_start < resp.length() && resp[rdb_start] == '$') {
                                size_t len_end = resp.find("\r\n", rdb_start);
                                if (len_end != std::string::npos) {
                                  int rdb_len = std::stoi(resp.substr(rdb_start + 1, len_end - rdb_start - 1));
                                  std::cout << "Expecting RDB file of " << rdb_len << " bytes\n";
                                  // Calculate how much of the RDB we've already received
                                  size_t rdb_data_start = len_end + 2;
                                  int received = n - rdb_data_start;
                                  int remaining = rdb_len - received;
                                  // Receive any remaining RDB data
                                  while (remaining > 0) {
                                    char rdb_buf[1024];
                                    ssize_t nr = recv(master_fd, rdb_buf, std::min(remaining, 1024), 0);
                                    if (nr <= 0) {
                                      std::cerr << "Failed to receive complete RDB file\n";
                                      break;
                                    }
                                    remaining -= nr;
                                  }
                                  std::cout << "Received complete RDB file from master\n";
                                  // Immediately add master_fd to select set and update fd_max
                                  FD_SET(master_fd, &master_set);
                                  if (master_fd > fd_max) fd_max = master_fd;
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
            }
          }
        } else {
          std::cerr << "Failed to connect to master at " << master_host << ":" << master_port << "\n";
          close(master_fd);
          master_fd = -1;
        }
      } else {
        std::cerr << "Failed to create socket for master connection\n";
      }
      freeaddrinfo(res);
    } else {
      std::cerr << "getaddrinfo failed for master host " << master_host << ":" << master_port << "\n";
    }
  }

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(port);

  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port " << port << "\n";
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
  // After handshake, if we need to add master_fd to select set
  if (add_master_fd_to_set && master_fd >= 0) {
    FD_SET(master_fd, &master_set);
    if (master_fd > fd_max) fd_max = master_fd;
  }

  std::cout << "Server event loop started. Waiting for clients...\n";

  // Transaction state per client fd
  std::unordered_map<int, bool> client_in_multi;
  // Queued commands per client fd
  std::unordered_map<int, std::vector<std::vector<std::string>>> client_multi_queue;
  // For this stage, we only need to track if MULTI was called, not queue commands
  while (true) {
    read_fds = master_set;
    // Compute select timeout for BLPOP and XREAD
    timeval *timeout_ptr = nullptr;
    timeval timeout_val;
    double min_timeout = -1;
    TimePoint now = Clock::now();
    
    // Check BLPOP timeouts
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
    
    // Check XREAD timeouts
    for (const auto& w : xread_waiting_clients) {
      if (w.timeout > 0) {
        double elapsed = std::chrono::duration<double>(now - w.start).count();
        double remain = w.timeout - elapsed;
        if (remain < 0) remain = 0;
        if (min_timeout < 0 || remain < min_timeout) min_timeout = remain;
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
    
    // Handle XREAD timeouts
    auto xread_it = xread_waiting_clients.begin();
    while (xread_it != xread_waiting_clients.end()) {
      const XreadWaiter& w = *xread_it;
      if (w.timeout > 0) {
        double elapsed = std::chrono::duration<double>(now - w.start).count();
        if (elapsed >= w.timeout) {
          // Timeout expired, respond with $-1\r\n
          write(w.fd, "$-1\r\n", 5);
          close(w.fd);
          FD_CLR(w.fd, &master_set);
          xread_it = xread_waiting_clients.erase(xread_it);
          continue;
        }
      }
      ++xread_it;
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
        } else if (is_replica && master_fd >= 0 && fd == master_fd) {
          // Data from master - process commands
          char buffer[1024] = {0};
          int bytes_read = read(fd, buffer, sizeof(buffer));
          if (bytes_read <= 0) {
            if (bytes_read < 0) std::cerr << "Failed to read from master\n";
            else std::cout << "Master disconnected\n";
            close(master_fd);
            FD_CLR(master_fd, &master_set);
            master_fd = -1;
          } else {
            // Process commands from master
            std::string request(buffer, bytes_read);
            // The master might send multiple commands in one buffer
            size_t pos = 0;
            while (pos < request.length()) {
              // Find the end of the current command
              size_t cmd_end = pos;
              if (request[pos] == '*') {
                // RESP array - need to parse full command
                size_t arr_end = request.find("\r\n", pos);
                if (arr_end == std::string::npos) break;
                int num_args = std::stoi(request.substr(pos + 1, arr_end - pos - 1));
                cmd_end = arr_end + 2;
                // Skip through all arguments
                for (int i = 0; i < num_args; i++) {
                  if (cmd_end >= request.length()) break;
                  if (request[cmd_end] == '$') {
                    size_t len_end = request.find("\r\n", cmd_end);
                    if (len_end == std::string::npos) break;
                    int arg_len = std::stoi(request.substr(cmd_end + 1, len_end - cmd_end - 1));
                    cmd_end = len_end + 2 + arg_len + 2; // $<len>\r\n<data>\r\n
                  }
                }
              }
              if (cmd_end > pos && cmd_end <= request.length()) {
                std::string single_cmd = request.substr(pos, cmd_end - pos);
                std::vector<std::string> args = RespParser::parseArray(single_cmd);
                if (!args.empty()) {
                  // Process the command from master
                  CommandHandler handler(kv_store, expiry_store, list_store);
                  std::string response = handler.handle(args); // Execute but don't send response to master
                  std::string cmd_upper = args[0];
                  std::transform(cmd_upper.begin(), cmd_upper.end(), cmd_upper.begin(), ::toupper);
                  std::cout << "Processed command from master: " << cmd_upper << std::endl;
                }
                pos = cmd_end;
              } else {
                break; // Incomplete command, wait for more data
              }
            }
          }
        } else {
          // Data from existing client (not master)
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
            // Clean up XREAD tracking if client disconnects
            auto xread_it = xread_waiting_clients.begin();
            while (xread_it != xread_waiting_clients.end()) {
              if (xread_it->fd == fd) {
                xread_it = xread_waiting_clients.erase(xread_it);
              } else {
                ++xread_it;
              }
            }
            // Clean up MULTI state
            client_in_multi.erase(fd);
            close(fd);
            FD_CLR(fd, &master_set);
          } else {
            std::string request(buffer, bytes_read);
            std::vector<std::string> args = RespParser::parseArray(request);
            // Convert command to uppercase for comparison
            std::string cmd_upper;
            if (!args.empty()) {
              cmd_upper = args[0];
              std::transform(cmd_upper.begin(), cmd_upper.end(), cmd_upper.begin(), ::toupper);
            }
            // Handle REPLCONF handshake from replicas
            if (!args.empty() && cmd_upper == "REPLCONF") {
              std::string response = "+OK\r\n";
              if (write(fd, response.c_str(), response.size()) < 0) {
                std::cerr << "Failed to send response to client fd=" << fd << "\n";
                close(fd);
                FD_CLR(fd, &master_set);
              }
              continue;
            }
            // Handle PSYNC from replicas (for replication handshake)
            if (!args.empty() && cmd_upper == "PSYNC") {
              // Respond with FULLRESYNC <runid> <offset> and a minimal RDB file
              std::string runid = "75cd7bc10c49047e0d163660f3b90625b1af31dc"; // static runid for test
              std::string fullresync = "+FULLRESYNC " + runid + " 0\r\n";
              if (write(fd, fullresync.c_str(), fullresync.size()) < 0) {
                std::cerr << "Failed to send FULLRESYNC to replica fd=" << fd << "\n";
                close(fd);
                FD_CLR(fd, &master_set);
                continue;
              }
              // Minimal RDB file (copied from test output, 88 bytes)
              std::string rdb = "$88\r\nREDIS0011\xFA\tredis-ver\x057.2.0\xFA\nredis-bits\xC0@\xFA\x05ctime\xC2m\x08\xBCe\xFA\x08used-mem\xB0\xC4\x10\x00\xFA\x0baof-base\xC0\x00\xFF\xF0n;\xFE\xC0\xFFZ\xA2";
              if (write(fd, rdb.c_str(), rdb.size()) < 0) {
                std::cerr << "Failed to send RDB to replica fd=" << fd << "\n";
                close(fd);
                FD_CLR(fd, &master_set);
              }
              continue;
            }
            // Normal command handling for clients
            CommandHandler handler(kv_store, expiry_store, list_store);
            std::string response = handler.handle(args);
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