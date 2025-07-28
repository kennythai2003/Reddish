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
  // Per-client input buffers for partial reads
  std::unordered_map<int, std::string> client_buffers;
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

  // Always bind and listen before any replica handshake, so server is ready for clients
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

  // If replica, connect to master and send PING handshake
  int master_fd = -1;
  std::string master_leftover; // Buffer for partial reads from master
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
          int flags = fcntl(master_fd, F_GETFL, 0);
          if (flags != -1) fcntl(master_fd, F_SETFL, flags & ~O_NONBLOCK);
          // Handshake
          std::string ping = "*1\r\n$4\r\nPING\r\n";
          send(master_fd, ping.c_str(), ping.size(), 0);
          char resp_buf[128] = {0};
          ssize_t n = recv(master_fd, resp_buf, sizeof(resp_buf)-1, 0);
          if (n > 0 && resp_buf[0] == '+') {
            std::string port_str2 = std::to_string(port);
            std::string replconf1 = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + std::to_string(port_str2.size()) + "\r\n" + port_str2 + "\r\n";
            send(master_fd, replconf1.c_str(), replconf1.size(), 0);
            char resp_buf2[128] = {0};
            ssize_t n2 = recv(master_fd, resp_buf2, sizeof(resp_buf2)-1, 0);
            if (n2 > 0 && resp_buf2[0] == '+') {
              std::string replconf2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
              send(master_fd, replconf2.c_str(), replconf2.size(), 0);
              char resp_buf3[128] = {0};
              ssize_t n3 = recv(master_fd, resp_buf3, sizeof(resp_buf3)-1, 0);
              if (n3 > 0 && resp_buf3[0] == '+') {
                std::string psync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
                send(master_fd, psync.c_str(), psync.size(), 0);
                // Wait for FULLRESYNC and RDB bulk string
                std::string rdb_data;
                bool rdb_done = false;
                while (!rdb_done) {
                  char buf[4096];
                  ssize_t r = recv(master_fd, buf, sizeof(buf), 0);
                  if (r <= 0) break;
                  rdb_data.append(buf, r);
                  // Look for end of RDB bulk string: $<len>\r\n...<binary>...
                  size_t dollar = rdb_data.find('$');
                  if (dollar != std::string::npos) {
                    size_t rn = rdb_data.find("\r\n", dollar);
                    if (rn != std::string::npos) {
                      size_t len = std::stoul(rdb_data.substr(dollar + 1, rn - dollar - 1));
                      size_t start = rn + 2;
                      if (rdb_data.size() >= start + len) {
                        // RDB file received, skip it
                        rdb_data = rdb_data.substr(start + len);
                        rdb_done = true;
                        break;
                      }
                    }
                  }
                }
              // Save any leftover data after RDB for event loop processing
              master_leftover = rdb_data;
              // Do NOT close master_fd; keep it open for event loop
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
    // After this, continue running the server to accept client connections
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
  if (master_fd >= 0) {
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
    // ...removed debug output...
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
          // ...existing code for new client connection...
          int new_client_fd = accept(server_fd, (struct sockaddr *)&client_addr, (socklen_t *)&client_addr_len);
          if (new_client_fd < 0) {
            std::cerr << "Failed to accept client connection\n";
            continue;
          }
          FD_SET(new_client_fd, &master_set);
          if (new_client_fd > fd_max) fd_max = new_client_fd;
          std::cout << "[DEBUG] Client connected: fd=" << new_client_fd << ", fd_max now " << fd_max << std::endl;
        } else if (is_replica && fd == master_fd) {
          // Data from master (as replica) - UPDATED SECTION
          char buffer[4096] = {0};
          int bytes_read = read(master_fd, buffer, sizeof(buffer));
          if (bytes_read <= 0) {
            std::cerr << "Master connection closed or error\n";
            close(master_fd);
            FD_CLR(master_fd, &master_set);
            master_fd = -1;
            continue;
          }
          
          // Append to master buffer
          master_leftover.append(buffer, bytes_read);
          
          // Parse as many complete RESP arrays as possible
          size_t pos = 0;
          while (pos < master_leftover.size()) {
            // Look for start of array
            size_t arr_start = master_leftover.find('*', pos);
            if (arr_start == std::string::npos) break;
            
            // Parse array count
            size_t rn = master_leftover.find("\r\n", arr_start);
            if (rn == std::string::npos) break; // Need more data
            
            int n_args = 0;
            try {
              n_args = std::stoi(master_leftover.substr(arr_start + 1, rn - arr_start - 1));
            } catch (...) { 
              pos = arr_start + 1; 
              continue; 
            }
            
            if (n_args <= 0) {
              pos = rn + 2;
              continue;
            }
            
            // Parse each argument
            std::vector<std::string> args;
            size_t cur = rn + 2;
            bool parse_complete = true;
            
            for (int i = 0; i < n_args; ++i) {
              // Expect bulk string: $<length>\r\n<data>\r\n
              if (cur >= master_leftover.size() || master_leftover[cur] != '$') {
                parse_complete = false;
                break;
              }
              
              // Find length line ending
              size_t len_end = master_leftover.find("\r\n", cur);
              if (len_end == std::string::npos) {
                parse_complete = false;
                break;
              }
              
              // Parse length
              int arglen = 0;
              try {
                arglen = std::stoi(master_leftover.substr(cur + 1, len_end - cur - 1));
              } catch (...) {
                parse_complete = false;
                break;
              }
              
              // Check if we have enough data for the argument
              size_t data_start = len_end + 2;
              if (data_start + arglen + 2 > master_leftover.size()) {
                parse_complete = false;
                break;
              }
              
              // Extract argument
              args.push_back(master_leftover.substr(data_start, arglen));
              cur = data_start + arglen + 2; // Skip past \r\n
            }
            
            if (!parse_complete) {
              // Not enough data for complete command, wait for more
              break;
            }
            
            // We have a complete command, process it
            if (!args.empty()) {
              std::cout << "[REPLICA] Processing command from master: " << args[0];
              for (size_t i = 1; i < args.size(); ++i) {
                std::cout << " " << args[i];
              }
              std::cout << std::endl;
              
              // Apply the command to local state (no response sent back to master)
              CommandHandler handler(kv_store, expiry_store, list_store);
              std::string response = handler.handle(args);
              
              // For debugging: log what the response would have been
              std::cout << "[REPLICA] Command processed, response would be: " << response.substr(0, 50);
              if (response.length() > 50) std::cout << "...";
              std::cout << std::endl;
            }
            
            // Move to next command
            pos = cur;
          }
          
          // Remove processed data from buffer
          if (pos > 0) {
            master_leftover = master_leftover.substr(pos);
          }
        } else {
          // ...existing code for client data...
          char buffer[1024] = {0};
          int bytes_read = read(fd, buffer, sizeof(buffer));
          if (bytes_read <= 0) {
            // ...existing code for client disconnect...
            if (bytes_read < 0) std::cerr << "failed to read from client fd=" << fd << "\n";
            else std::cout << "Client disconnected: fd=" << fd << "\n";
            // ...existing code for cleaning up client state...
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
            auto xread_it = xread_waiting_clients.begin();
            while (xread_it != xread_waiting_clients.end()) {
              if (xread_it->fd == fd) {
                xread_it = xread_waiting_clients.erase(xread_it);
              } else {
                ++xread_it;
              }
            }
            client_in_multi.erase(fd);
            client_buffers.erase(fd);
            close(fd);
            FD_CLR(fd, &master_set);
          } else {
            // ...existing code for client buffer and command handling...
            client_buffers[fd].append(buffer, bytes_read);
            std::string& buf = client_buffers[fd];
            size_t pos = 0;
            while (pos < buf.size()) {
              // ...existing code for parsing and handling RESP arrays from client...
              size_t arr_start = buf.find('*', pos);
              if (arr_start == std::string::npos) break;
              size_t rn = buf.find("\r\n", arr_start);
              if (rn == std::string::npos) break;
              int n_args = 0;
              try {
                n_args = std::stoi(buf.substr(arr_start + 1, rn - arr_start - 1));
              } catch (...) { pos = arr_start + 1; continue; }
              std::vector<std::string> args;
              size_t cur = rn + 2;
              bool parse_fail = false;
              for (int i = 0; i < n_args; ++i) {
                if (cur >= buf.size() || buf[cur] != '$') { parse_fail = true; break; }
                size_t rn1 = buf.find("\r\n", cur);
                if (rn1 == std::string::npos) { parse_fail = true; break; }
                int arglen = 0;
                try {
                  arglen = std::stoi(buf.substr(cur + 1, rn1 - cur - 1));
                } catch (...) { parse_fail = true; break; }
                size_t start = rn1 + 2;
                if (start + arglen > buf.size()) { parse_fail = true; break; }
                args.push_back(buf.substr(start, arglen));
                cur = start + arglen + 2; // skip \r\n
              }
              if (parse_fail) {
                pos = arr_start + 1;
                continue;
              }
              std::string cmd_upper;
              if (!args.empty()) {
                cmd_upper = args[0];
                std::transform(cmd_upper.begin(), cmd_upper.end(), cmd_upper.begin(), ::toupper);
              }
              // Handle client commands
              if (!args.empty()) {
                std::cout << "[CLIENT] Processing command: " << args[0];
                for (size_t i = 1; i < args.size(); ++i) {
                  std::cout << " " << args[i];
                }
                std::cout << std::endl;
                
                // Process the command and send response back to client
                CommandHandler handler(kv_store, expiry_store, list_store);
                std::string response = handler.handle(args);
                
                std::cout << "[CLIENT] Sending response: " << response.substr(0, 50);
                if (response.length() > 50) std::cout << "...";
                std::cout << std::endl;
                
                // Send response back to client
                ssize_t sent = write(fd, response.c_str(), response.size());
                if (sent < 0) {
                  std::cerr << "Failed to send response to client fd=" << fd << std::endl;
                }
                
                // Special handling for PSYNC - send RDB file after FULLRESYNC response
                std::string cmd_upper = args[0];
                std::transform(cmd_upper.begin(), cmd_upper.end(), cmd_upper.begin(), ::toupper);
                if (cmd_upper == "PSYNC" && args.size() == 3) {
                  // Send empty RDB file after FULLRESYNC response
                  std::string rdb_content = "REDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\b\xbce\xfa\bused-mem°\xc4\x10\x00\xfa\baof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2";
                  std::string rdb_header = "$" + std::to_string(rdb_content.size()) + "\r\n";
                  std::string full_rdb = rdb_header + rdb_content;
                  
                  ssize_t rdb_sent = write(fd, full_rdb.c_str(), full_rdb.size());
                  if (rdb_sent < 0) {
                    std::cerr << "Failed to send RDB file to replica fd=" << fd << std::endl;
                  } else {
                    std::cout << "[MASTER] Sent RDB file to replica fd=" << fd << std::endl;
                    // Track this fd as a replica for future command propagation
                    replica_fds.push_back(fd);
                  }
                }
              // Propagate write commands to all replicas
              // Only propagate after handshake (i.e., fd is not a replica)
              // Only propagate write commands (SET, RPUSH, LPUSH, XADD, INCR, etc.)
              static const std::vector<std::string> write_cmds = {"SET", "RPUSH", "LPUSH", "XADD", "INCR", "DEL"};
              if (std::find(write_cmds.begin(), write_cmds.end(), cmd_upper) != write_cmds.end() && !replica_fds.empty()) {
                // Reconstruct the original RESP command
                std::string resp_cmd = "*" + std::to_string(args.size()) + "\r\n";
                for (const auto& arg : args) {
                  resp_cmd += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
                }
                // Send to all replicas
                for (auto it = replica_fds.begin(); it != replica_fds.end(); ) {
                  int rfd = *it;
                  ssize_t w = write(rfd, resp_cmd.c_str(), resp_cmd.size());
                  if (w < 0) {
                    std::cerr << "Failed to propagate to replica fd=" << rfd << ", removing from list." << std::endl;
                    close(rfd);
                    FD_CLR(rfd, &master_set);
                    it = replica_fds.erase(it);
                  } else {
                    ++it;
                  }
                }
              }
              }
              pos = cur;
            }
            if (pos > 0) buf = buf.substr(pos);
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