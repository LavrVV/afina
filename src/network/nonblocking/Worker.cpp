#include "Worker.h"

#include <iostream>

#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <protocol/Parser.h>
#include <map>

#include "Utils.h"

#include <afina/Executor.h>
#include <afina/execute/Command.h>

#define MAXEVENTS 64

namespace Afina {
namespace Network {
namespace NonBlocking {

// See Worker.h
Worker::Worker(std::shared_ptr<Afina::Storage> ps):isRunning(0) {
    // TODO: implementation here
}

// See Worker.h
Worker::~Worker() {
    // TODO: implementation here
}

// See Worker.h
void Worker::Start(int server_socket) {
    std::cout << "network debug: " << __PRETTY_FUNCTION__ << std::endl;
    // TODO: implementation here
	pthread_create(this->thread, NULL, this->OnRun, &server_socket);
	this->isRunning.store(1);
}

// See Worker.h
void Worker::Stop() {
    std::cout << "network debug: " << __PRETTY_FUNCTION__ << std::endl;
    // TODO: implementation here
	this->isRunning.store(0);
}

// See Worker.h
void Worker::Join() {
    std::cout << "network debug: " << __PRETTY_FUNCTION__ << std::endl;
    // TODO: implementation here
	pthread_join(this->thread, NULL);
}

// See Worker.h
void Worker::OnRun(void *args) {
    std::cout << "network debug: " << __PRETTY_FUNCTION__ << std::endl;

    // TODO: implementation here
    // 1. Create epoll_context here
    // 2. Add server_socket to context
    // 3. Accept new connections, don't forget to call make_socket_nonblocking on
    //    the client socket descriptor
    // 4. Add connections to the local context
    // 5. Process connection events
    //
    // Do not forget to use EPOLLEXCLUSIVE flag when register socket
    // for events to avoid thundering herd type behavior.
	
		map<int, std::unique_ptr<Execute::Command>> commands;
		map<int, uint32_t> body_sizes;
				int sfd = args[0];
		int epfd = epoll_create(MAXEVENTS);
				if (epfd == -1) {
			throw std::runtime_error("cannot epoll_create");
		}
		epoll_event events[MAXEVENTS];
		epoll_event ev;
		ev.events = EPOLLEXCLUSIVE | EPOLLHUP | EPOLLIN | EPOLLERR;
		ev.data.fd = sfd; 
				if (epoll_ctl(epfd, EPOLL_CTL_ADD, sfd, &ev) == -1) {
			throw std::runtime_error("cannot epoll_ctl");
		}
				//int* args = new int {sfd};
		pthread_cleanup_push(Worker::Clean, args);
		while (running.load()) {
			int n = epoll_wait(epfd, events, MAXEVENTS, -1);
						if (n == -1) {
				throw std::runtime_error("error in epoll_wait");
			}
						for (int i = 0; i < n; i++) {
				int ev_soc = events[i].data.fd;
				// error or hangup
				if (((events[i].events & EPOLLERR) == EPOLLERR) || ((events[i].events & EPOLLHUP) == EPOLLHUP))  {
					cerr << "Error with socket connection\n";
										epoll_ctl(epfd, EPOLL_CTL_DEL, ev_soc, NULL);
					close(ev_soc);
										body_sizes.erase(ev_soc);
					continue;
				}
				// event with server
				if (events[i].data.fd == sfd) {
					// client connected
					if ((events[i].events & EPOLLIN) == EPOLLIN) {
						int socket = accept(sfd, NULL, NULL);
												make_socket_non_blocking(socket);
						ev.events = EPOLLIN | EPOLLRDHUP;
						ev.data.fd = socket;
						if (epoll_ctl(epfd, EPOLL_CTL_ADD, socket, &ev) == -1) {
							throw std::runtime_error("error in epoll_ctl");
						}
						continue;
					}
									continue;
				}
				// event not with server
				if ((events[i].events & EPOLLIN) == EPOLLIN) {
					ssize_t s = 0;
					char buf[512];
					bool need_clean = false;
					string data = "";
					// read all data
					while (true) {
						s = read(ev_soc, buf, sizeof(buf));
						if (s == -1) {
							if (errno != EAGAIN) {
								need_clean = true;
							}
							break;
						} else if (s == 0) {
							need_clean = true;
							break;
						}
						buf[s] = '\0';
						data += string(buf);
					}
					if (need_clean) {
						cerr << "Unknown error with reading data.\n";
						epoll_ctl(epfd, EPOLL_CTL_DEL, ev_soc, NULL);
						close(ev_soc);
						commands.erase(ev_soc);
						body_sizes.erase(ev_soc);
						continue;
					}
					// execute commands
					while (true) {
						if (data.length() < 3) {
							break;
						}
						// command not built yet
						if (body_sizes.find(ev_soc) == body_sizes.end()) {
							try {
								int parsed;
								Protocol::Parse parser();
								if (!parser.Parse(data.data(), data.length(), parsed)) {
									break;
								} else {
									cout << "Parsed: " << parsed << endl;
									cout << "Buffer: " << data << endl;
									data.erase(0, parsed);
									parsed = 0;
									uint32_t body_size;
									auto command = parser.Build(body_size);
									commands[ev_soc] = move(command);
									body_sizes[ev_soc] = body_size;
								}
							} catch(...) {
								cerr << "Error: Command not built and cant parse input\n";
								cerr << "Buffer: " << data << endl;
								cerr << "Length: " << data << endl;
								string ret = "ERROR\r\n";
								send(ev_soc, ret.data(), ret.size(), 0);
								epoll_ctl(epfd, EPOLL_CTL_DEL, ev_soc, NULL);
								close(ev_soc);
								commands.erase(ev_soc);
								body_sizes.erase(ev_soc);
								break;
							}
						}
						// command already built
						cout << "Left to parse: " << data << endl;
						if (data.length() < body_sizes[ev_soc]) {
							break;
						}
						string ret;
						string args;
						copy(buffers[ev_soc].begin(), buffers[ev_soc].begin() + body_sizes[ev_soc], back_inserter(args));
						buffers[ev_soc].erase(0, body_sizes[ev_soc]);
						if (data.length() > 0 && (data[0] == '\r' || data[0] == '\n')) {
							data.erase(data.begin());
						}
						if (data.length() > 0 && (data[0] == '\r' || data[0] == '\n')) {
							data.erase(data.begin());
						}
						try {
							commands[ev_soc]->Execute(*(this->ps), args, ret);
						} catch(...) {
							ret = "ERROR";
						}
						ret += "\r\n";
						cout << "Return: " << ret << endl;
						if (send(ev_soc, ret.data(), ret.size(), 0) <= 0) {
							epoll_ctl(epfd, EPOLL_CTL_DEL, ev_soc, NULL);
							close(ev_soc);
							commands.erase(ev_soc);
							body_sizes.erase(ev_soc);
							break;
						}
												body_sizes.erase(ev_soc);
						commands.erase(ev_soc);
					}	
				}
			}
		}
		pthread_cleanup_pop(1);
		pthread_exit(NULL);
}

} // namespace NonBlocking
} // namespace Network
} // namespace Afina
