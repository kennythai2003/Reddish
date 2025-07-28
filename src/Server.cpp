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
  // If replica, connect to master and send PING handshake
  int master_fd = -1;
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

            // REPLCONF handshake support (master side)
            if (!args.empty() && cmd_upper == "REPLCONF") {
              std::string response = "+OK\r\n";
              if (write(fd, response.c_str(), response.size()) < 0) {
                std::cerr << "Failed to send response to client fd=" << fd << "\n";
                close(fd);
                FD_CLR(fd, &master_set);
              }
              continue;
            }
            // PSYNC handshake support (master side)
            if (args.size() == 3 && cmd_upper == "PSYNC" && args[1] == "?" && args[2] == "-1") {
              std::string replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
              std::string response = "+FULLRESYNC " + replid + " 0\r\n";
              if (write(fd, response.c_str(), response.size()) < 0) {
                std::cerr << "Failed to send response to client fd=" << fd << "\n";
                close(fd);
                FD_CLR(fd, &master_set);
                continue;
              }
              // Use a valid empty RDB file as a std::string, as in the reference code
              const std::string empty_rdb =
                "\x52\x45\x44\x49\x53\x30\x30\x31\x31\xfa\x09\x72\x65\x64\x69\x73\x2d\x76\x65\x72\x05\x37\x2e\x32\x2e\x30\xfa\x0a\x72\x65\x64\x69\x73\x2d\x62\x69\x74\x73\xc0\x40\xfa\x05\x63\x74\x69\x6d\x65\xc2\x6d\x08\xbc\x65\xfa\x08\x75\x73\x65\x64\x2d\x6d\x65\x6d\xc2\xb0\xc4\x10\x00\xfa\x08\x61\x6f\x66\x2d\x62\x61\x73\x65\xc0\x00\xff\xf0\x6e\x3b\xfe\xc0\xff\x5a\xa2";
              std::string rdb_header = "$" + std::to_string(empty_rdb.length()) + "\r\n";
              if (write(fd, rdb_header.c_str(), rdb_header.size()) < 0) {
                std::cerr << "Failed to send RDB header to client fd=" << fd << "\n";
                close(fd);
                FD_CLR(fd, &master_set);
                continue;
              }
              if (write(fd, empty_rdb.data(), empty_rdb.length()) < 0) {
                std::cerr << "Failed to send RDB body to client fd=" << fd << "\n";
                close(fd);
                FD_CLR(fd, &master_set);
                continue;
              }
              // Add this fd to the list of replica connections
              replica_fds.push_back(fd);
              continue;
            }
            // Transaction support: MULTI/EXEC
            if (!args.empty() && cmd_upper == "MULTI") {
              client_in_multi[fd] = true;
              client_multi_queue[fd].clear();
              std::string response = "+OK\r\n";
              if (write(fd, response.c_str(), response.size()) < 0) {
                std::cerr << "Failed to send response to client fd=" << fd << "\n";
                close(fd);
                FD_CLR(fd, &master_set);
              }
              continue;
            }
            if (!args.empty() && client_in_multi.count(fd) && client_in_multi[fd] && cmd_upper != "EXEC") {
              // DISCARD should not be queued, handle immediately
              if (cmd_upper == "DISCARD") {
                client_in_multi[fd] = false;
                client_multi_queue[fd].clear();
                std::string response = "+OK\r\n";
                if (write(fd, response.c_str(), response.size()) < 0) {
                  std::cerr << "Failed to send response to client fd=" << fd << "\n";
                  close(fd);
                  FD_CLR(fd, &master_set);
                }
                continue;
              } else {
                // Queue command, respond with +QUEUED, do not execute
                client_multi_queue[fd].push_back(args);
                std::string response = "+QUEUED\r\n";
                if (write(fd, response.c_str(), response.size()) < 0) {
                  std::cerr << "Failed to send response to client fd=" << fd << "\n";
                  close(fd);
                  FD_CLR(fd, &master_set);
                }
                continue;
              }
            }
            if (!args.empty() && cmd_upper == "EXEC") {
              if (client_in_multi.count(fd) && client_in_multi[fd]) {
                // Execute queued commands, collect responses (including errors)
                std::vector<std::string> responses;
                for (const auto& qargs : client_multi_queue[fd]) {
                  CommandHandler handler(kv_store, expiry_store, list_store);
                  std::string resp = handler.handle(qargs);
                  // If response is empty, treat as null bulk string
                  if (resp.empty()) {
                    responses.push_back("$-1\r\n");
                  } else if (!resp.empty() && resp[0] == '-') {
                    // Error response, keep as is
                    responses.push_back(resp);
                  } else {
                    responses.push_back(resp);
                  }
                }
                // RESP array of responses
                std::string response = "*" + std::to_string(responses.size()) + "\r\n";
                for (const auto& r : responses) {
                  response += r;
                }
                client_in_multi[fd] = false;
                client_multi_queue[fd].clear();
                if (write(fd, response.c_str(), response.size()) < 0) {
                  std::cerr << "Failed to send response to client fd=" << fd << "\n";
                  close(fd);
                  FD_CLR(fd, &master_set);
                }
              } else {
                std::string response = "-ERR EXEC without MULTI\r\n";
                if (write(fd, response.c_str(), response.size()) < 0) {
                  std::cerr << "Failed to send response to client fd=" << fd << "\n";
                  close(fd);
                  FD_CLR(fd, &master_set);
                }
              }
              continue;
            }
            
            // DISCARD handling
            if (!args.empty() && cmd_upper == "DISCARD") {
              if (client_in_multi.count(fd) && client_in_multi[fd]) {
                client_in_multi[fd] = false;
                client_multi_queue[fd].clear();
                std::string response = "+OK\r\n";
                if (write(fd, response.c_str(), response.size()) < 0) {
                  std::cerr << "Failed to send response to client fd=" << fd << "\n";
                  close(fd);
                  FD_CLR(fd, &master_set);
                }
              } else {
                std::string response = "-ERR DISCARD without MULTI\r\n";
                if (write(fd, response.c_str(), response.size()) < 0) {
                  std::cerr << "Failed to send response to client fd=" << fd << "\n";
                  close(fd);
                  FD_CLR(fd, &master_set);
                }
              }
              continue;
            }
            
            // BLPOP handling
            if (!args.empty() && cmd_upper == "BLPOP" && args.size() == 3) {
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
            
            // XREAD blocking handling
            if (!args.empty() && args.size() >= 4) {
              if (cmd_upper == "XREAD" && args[1] == "block") {
                double timeout = 0;
                try { timeout = std::stod(args[2]) / 1000.0; } catch (...) { timeout = 0; } // Convert ms to seconds

                if (args[3] != "streams") {
                  std::string response = "-ERR syntax error\r\n";
                  if (write(fd, response.c_str(), response.size()) < 0) {
                    std::cerr << "Failed to send response to client fd=" << fd << "\n";
                    close(fd);
                    FD_CLR(fd, &master_set);
                  }
                  continue;
                }

                // Parse streams and IDs
                int n_streams = (args.size() - 4) / 2;
                if ((args.size() - 4) % 2 != 0 || n_streams < 1) {
                  std::string response = "-ERR wrong number of arguments for 'XREAD' command\r\n";
                  if (write(fd, response.c_str(), response.size()) < 0) {
                    std::cerr << "Failed to send response to client fd=" << fd << "\n";
                    close(fd);
                    FD_CLR(fd, &master_set);
                  }
                  continue;
                }

                std::vector<std::string> stream_keys;
                std::vector<std::string> last_ids;
                for (int i = 4; i < 4 + n_streams; ++i) {
                  stream_keys.push_back(args[i]);
                }
                for (int i = 4 + n_streams; i < 4 + 2 * n_streams; ++i) {
                  last_ids.push_back(args[i]);
                }

                // If $ is passed as the ID, replace it with the highest ID in the stream at the time of blocking
                for (int s = 0; s < n_streams; ++s) {
                  if (last_ids[s] == "$") {
                    auto it = stream_store.find(stream_keys[s]);
                    if (it != stream_store.end() && !it->second.empty()) {
                      last_ids[s] = it->second.back().id;
                    } else {
                      last_ids[s] = "0-0";
                    }
                  }
                }

                // Check if there are already new entries
                std::vector<std::string> xread_args = {"XREAD", "streams"};
                xread_args.insert(xread_args.end(), stream_keys.begin(), stream_keys.end());
                xread_args.insert(xread_args.end(), last_ids.begin(), last_ids.end());

                CommandHandler check_handler(kv_store, expiry_store, list_store);
                std::string response = check_handler.handle(xread_args);

                // Debug output
                std::cout << "XREAD check response: " << response << std::endl;

                // Check if response contains actual data (not empty streams)
                bool has_data = false;
                // Count how many streams have non-zero entries
                size_t pos = 0;
                for (int i = 0; i < n_streams; ++i) {
                  // Find pattern: stream_name followed by entries array
                  std::string stream_name = stream_keys[i];
                  std::string stream_pattern = "$" + std::to_string(stream_name.size()) + "\r\n" + stream_name + "\r\n*";
                  size_t stream_pos = response.find(stream_pattern, pos);
                  if (stream_pos != std::string::npos) {
                    // Extract the number after the last '*'
                    size_t count_start = stream_pos + stream_pattern.size();
                    size_t count_end = response.find("\r\n", count_start);
                    if (count_end != std::string::npos) {
                      std::string count_str = response.substr(count_start, count_end - count_start);
                      if (count_str != "0") {
                        has_data = true;
                        break;
                      }
                    }
                    pos = count_end;
                  }
                }

                if (has_data) {
                  // Send immediate response
                  if (write(fd, response.c_str(), response.size()) < 0) {
                    std::cerr << "Failed to send response to client fd=" << fd << "\n";
                    close(fd);
                    FD_CLR(fd, &master_set);
                  }
                } else {
                  // Block this client, storing the resolved last_ids
                  XreadWaiter w{fd, Clock::now(), timeout, stream_keys, last_ids};
                  xread_waiting_clients.push_back(w);
                }
                continue;
              }
            }
            
            // INFO replication handling
            if (!args.empty() && cmd_upper == "INFO" && args.size() >= 2 && args[1] == "replication") {
              // Hardcoded 40-char master_replid and offset 0
              std::string master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
              std::string master_repl_offset = "0";
              std::string info;
              if (is_replica) {
                info = "role:slave\n";
              } else {
                info = "role:master\n";
                info += "master_replid:" + master_replid + "\n";
                info += "master_repl_offset:" + master_repl_offset + "\n";
              }
              // Remove trailing newline for RESP length
              if (!info.empty() && info.back() == '\n') info.pop_back();
              std::string response = "$" + std::to_string(info.size()) + "\r\n" + info + "\r\n";
              if (write(fd, response.c_str(), response.size()) < 0) {
                std::cerr << "Failed to send response to client fd=" << fd << "\n";
                close(fd);
                FD_CLR(fd, &master_set);
              }
              continue;
            }
            
            // Normal command handling
            CommandHandler handler(kv_store, expiry_store, list_store);
            std::string response = handler.handle(args);

            // Only propagate if command was successful (not an error response)
            bool is_error = !response.empty() && response[0] == '-';

            // Propagate write commands to replica if connected
            // Only propagate if not from a replica itself and command succeeded
            static const std::vector<std::string> write_cmds = {"SET", "DEL", "LPUSH", "RPUSH", "EXPIRE", "PEXPIRE", "INCR", "DECR", "XADD"};
            if (!is_error && !args.empty() && !cmd_upper.empty() && std::find(write_cmds.begin(), write_cmds.end(), cmd_upper) != write_cmds.end()) {
                // Check if this fd is a replica (don't propagate commands from replicas)
                bool is_from_replica = std::find(replica_fds.begin(), replica_fds.end(), fd) != replica_fds.end();
                if (!is_from_replica) {
                    // Reconstruct the RESP array for the command
                    std::string resp = "*" + std::to_string(args.size()) + "\r\n";
                    for (const auto& arg : args) {
                        resp += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
                    }
                    // Send to all replicas
                    for (auto it = replica_fds.begin(); it != replica_fds.end(); ) {
                        int rfd = *it;
                        if (write(rfd, resp.c_str(), resp.size()) < 0) {
                            std::cerr << "Failed to propagate to replica fd=" << rfd << "\n";
                            // Remove closed/broken replica fds
                            close(rfd);
                            FD_CLR(rfd, &master_set);
                            it = replica_fds.erase(it);
                        } else {
                            ++it;
                        }
                    }
                }
            }

            // After RPUSH/LPUSH, check for blocked BLPOP clients
            if (!args.empty() && (cmd_upper == "RPUSH" || cmd_upper == "LPUSH") && args.size() >= 3) {
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

            // After XADD, check for blocked XREAD clients
            if (!args.empty() && cmd_upper == "XADD" && args.size() >= 5) {
                std::string stream_key = args[1];
                std::cout << "XADD detected for stream: " << stream_key << ", checking " << xread_waiting_clients.size() << " waiting clients\n";
                auto xread_it = xread_waiting_clients.begin();
                while (xread_it != xread_waiting_clients.end()) {
                    XreadWaiter& w = *xread_it;
                    bool stream_matches = false;
                    for (const std::string& key : w.stream_keys) {
                        if (key == stream_key) {
                            stream_matches = true;
                            break;
                        }
                    }
                    if (stream_matches) {
                        std::cout << "Found matching client for stream " << stream_key << ", fd=" << w.fd << std::endl;
                        // Generate XREAD response for this client
                        std::vector<std::string> xread_args = {"XREAD", "streams"};
                        xread_args.insert(xread_args.end(), w.stream_keys.begin(), w.stream_keys.end());
                        xread_args.insert(xread_args.end(), w.last_ids.begin(), w.last_ids.end());
                        CommandHandler xread_handler(kv_store, expiry_store, list_store);
                        std::string xread_response = xread_handler.handle(xread_args);
                        std::cout << "Sending XREAD response: " << xread_response << std::endl;
                        if (write(w.fd, xread_response.c_str(), xread_response.size()) < 0) {
                            std::cerr << "Failed to send response to client fd=" << w.fd << "\n";
                            close(w.fd);
                            FD_CLR(w.fd, &master_set);
                        }
                        xread_it = xread_waiting_clients.erase(xread_it);
                    } else {
                        ++xread_it;
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
            
            // After RPUSH/LPUSH, check for blocked BLPOP clients
            if (!args.empty() && (cmd_upper == "RPUSH" || cmd_upper == "LPUSH") && args.size() >= 3) {
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
            
            // After XADD, check for blocked XREAD clients
            if (!args.empty() && cmd_upper == "XADD" && args.size() >= 5) {
              std::string stream_key = args[1];
              std::cout << "XADD detected for stream: " << stream_key << ", checking " << xread_waiting_clients.size() << " waiting clients\n";
              
              auto xread_it = xread_waiting_clients.begin();
              while (xread_it != xread_waiting_clients.end()) {
                XreadWaiter& w = *xread_it;
                bool stream_matches = false;
                for (const std::string& key : w.stream_keys) {
                  if (key == stream_key) {
                    stream_matches = true;
                    break;
                  }
                }
                
                if (stream_matches) {
                  std::cout << "Found matching client for stream " << stream_key << ", fd=" << w.fd << std::endl;
                  // Generate XREAD response for this client
                  std::vector<std::string> xread_args = {"XREAD", "streams"};
                  xread_args.insert(xread_args.end(), w.stream_keys.begin(), w.stream_keys.end());
                  xread_args.insert(xread_args.end(), w.last_ids.begin(), w.last_ids.end());
                  
                  CommandHandler xread_handler(kv_store, expiry_store, list_store);
                  std::string xread_response = xread_handler.handle(xread_args);
                  
                  std::cout << "Sending XREAD response: " << xread_response << std::endl;
                  
                  if (write(w.fd, xread_response.c_str(), xread_response.size()) < 0) {
                    std::cerr << "Failed to send response to client fd=" << w.fd << "\n";
                    close(w.fd);
                    FD_CLR(w.fd, &master_set);
                  }
                  
                  xread_it = xread_waiting_clients.erase(xread_it);
                } else {
                  ++xread_it;
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