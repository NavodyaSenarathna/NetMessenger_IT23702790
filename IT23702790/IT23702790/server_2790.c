#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <pthread.h>
#include <time.h>
#include <errno.h>
#include <signal.h>

#define PORT 8790
#define NID_TAG "NID:7027"
#define LOG_FILE "netmsg_IT23702790.log"
#define STORAGE_BASE "./storage/IT23702790"
#define MAX_CLIENTS 50
#define MAX_ROOMS 20
#define BUFFER_SIZE 2048
#define OUT_SIZE 4096
#define MAX_FILE_SIZE (10L * 1024 * 1024)   /* 10 MB limit -> ERR 004 */

typedef struct {
    int socket;
    char username[64];
    int registered;
} Client;

typedef struct {
    char name[64];
    char members[MAX_CLIENTS][64];
    int member_count;
} Room;

Client clients[MAX_CLIENTS];
Room rooms[MAX_ROOMS];
int room_count = 0;
pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;

void log_event(const char *event_type, const char *details) {
    FILE *f = fopen(LOG_FILE, "a");
    if (!f) return;
    time_t now = time(NULL);
    char time_str[30];
    strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", localtime(&now));
    fprintf(f, "[%s] %s: %s\n", time_str, event_type, details);
    fclose(f);
}

int send_all(int sock, const char *data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(sock, data + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) return -1;
        sent += n;
    }
    return 0;
}

void send_response(int sock, const char *msg) {
    char response[OUT_SIZE];
    snprintf(response, sizeof(response), "%s %s\n", msg, NID_TAG);
    send_all(sock, response, strlen(response));
}

void forward_message(int sock, const char *msg) {
    char response[OUT_SIZE];
    snprintf(response, sizeof(response), "%s\n", msg);
    send_all(sock, response, strlen(response));
}

void create_dir_if_not_exists(const char *path) {
    char temp[512];
    snprintf(temp, sizeof(temp), "%s", path);
    for (char *p = temp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            mkdir(temp, 0777);
            *p = '/';
        }
    }
    mkdir(temp, 0777);
}

int read_line(int sock, char *buffer, size_t max_len) {
    size_t bytes_read = 0;
    while (bytes_read < max_len - 1) {
        char c;
        ssize_t res = recv(sock, &c, 1, 0);
        if (res <= 0) return -1;          /* -1 = peer closed or error */
        if (c == '\n') break;
        buffer[bytes_read++] = c;
    }
    buffer[bytes_read] = '\0';
    if (bytes_read > 0 && buffer[bytes_read - 1] == '\r') {
        buffer[bytes_read - 1] = '\0';
    }
    return bytes_read;
}

int find_client_by_name(const char *name) {
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].registered && strcmp(clients[i].username, name) == 0) {
            return i;
        }
    }
    return -1;
}

int find_room_by_name(const char *name) {
    for (int i = 0; i < room_count; i++) {
        if (strcmp(rooms[i].name, name) == 0) return i;
    }
    return -1;
}

void remove_client_from_rooms(const char *username) {
    for (int i = 0; i < room_count; i++) {
        for (int j = 0; j < rooms[i].member_count; j++) {
            if (strcmp(rooms[i].members[j], username) == 0) {
                for (int k = j; k < rooms[i].member_count - 1; k++) {
                    strcpy(rooms[i].members[k], rooms[i].members[k + 1]);
                }
                rooms[i].member_count--;
                break;
            }
        }
    }
}

int room_has_member(int r_idx, const char *username) {
    for (int m = 0; m < rooms[r_idx].member_count; m++) {
        if (strcmp(rooms[r_idx].members[m], username) == 0) return m;
    }
    return -1;
}

/* strip any directory part so a filename can never escape the storage folder */
const char *safe_basename(const char *name) {
    const char *p = strrchr(name, '/');
    return p ? p + 1 : name;
}

void broadcast_presence(const char *msg, int exclude_sock) {
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].registered && clients[i].socket != exclude_sock) {
            forward_message(clients[i].socket, msg);
        }
    }
}

void *handle_client(void *arg) {
    int client_idx = *(int *)arg;
    free(arg);
    int sock = clients[client_idx].socket;
    char buffer[BUFFER_SIZE];
    char log_buf[OUT_SIZE];

    while (1) {
        int res = read_line(sock, buffer, sizeof(buffer));
        if (res < 0) break;                 /* disconnect / error */
        if (res == 0) {                     /* blank line */
            send_response(sock, "ERR 006 EMPTY_COMMAND");
            continue;
        }

        if (strncmp(buffer, "REGISTER ", 9) == 0) {
            char username[64] = "";
            sscanf(buffer + 9, "%63s", username);

            if (username[0] == '\0' || strchr(username, ',')) {
                send_response(sock, "ERR 007 BAD_FORMAT");
                continue;
            }
            pthread_mutex_lock(&clients_mutex);
            if (clients[client_idx].registered) {
                send_response(sock, "ERR 008 ALREADY_REGISTERED");
            } else if (find_client_by_name(username) != -1) {
                send_response(sock, "ERR 001 USERNAME_TAKEN");
            } else {
                strcpy(clients[client_idx].username, username);
                clients[client_idx].registered = 1;
                char resp[128];
                snprintf(resp, sizeof(resp), "OK REGISTERED %s", username);
                send_response(sock, resp);

                snprintf(log_buf, sizeof(log_buf), "User registered: %s", username);
                log_event("REGISTER", log_buf);

                snprintf(resp, sizeof(resp), "MSG SYS User %s joined", username);
                broadcast_presence(resp, sock);
            }
            pthread_mutex_unlock(&clients_mutex);
        } else if (!clients[client_idx].registered) {
            send_response(sock, "ERR 005 NOT_REGISTERED");
        } else if (strcmp(buffer, "LIST") == 0) {
            pthread_mutex_lock(&clients_mutex);
            char list_resp[OUT_SIZE] = "OK USERS ";
            int first = 1;
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].registered) {
                    if (!first) strcat(list_resp, ",");
                    strcat(list_resp, clients[i].username);
                    first = 0;
                }
            }
            send_response(sock, list_resp);
            pthread_mutex_unlock(&clients_mutex);
        } else if (strncmp(buffer, "BCAST ", 6) == 0) {
            char *msg = buffer + 6;
            pthread_mutex_lock(&clients_mutex);
            send_response(sock, "OK SENT");
            char bmsg[OUT_SIZE];
            snprintf(bmsg, sizeof(bmsg), "MSG BCAST %s %s", clients[client_idx].username, msg);
            broadcast_presence(bmsg, sock);
            snprintf(log_buf, sizeof(log_buf), "%s -> ALL: %s", clients[client_idx].username, msg);
            log_event("BCAST", log_buf);
            pthread_mutex_unlock(&clients_mutex);
        } else if (strncmp(buffer, "PMSG ", 5) == 0) {
            char target[64], msg[BUFFER_SIZE];
            if (sscanf(buffer + 5, "%63s %[^\n]", target, msg) == 2) {
                pthread_mutex_lock(&clients_mutex);
                int target_idx = find_client_by_name(target);
                if (target_idx != -1) {
                    send_response(sock, "OK SENT");
                    char pmsg[OUT_SIZE];
                    snprintf(pmsg, sizeof(pmsg), "MSG PRIV %s %s", clients[client_idx].username, msg);
                    forward_message(clients[target_idx].socket, pmsg);
                    snprintf(log_buf, sizeof(log_buf), "%s -> %s: %s", clients[client_idx].username, target, msg);
                    log_event("PMSG", log_buf);
                } else {
                    send_response(sock, "ERR 002 USER_NOT_FOUND");
                }
                pthread_mutex_unlock(&clients_mutex);
            } else {
                send_response(sock, "ERR 007 BAD_FORMAT");
            }
        } else if (strncmp(buffer, "JOIN ", 5) == 0) {
            char room_name[64] = "";
            sscanf(buffer + 5, "%63s", room_name);
            if (room_name[0] == '\0' || strchr(room_name, ',')) {
                send_response(sock, "ERR 007 BAD_FORMAT");
                continue;
            }
            pthread_mutex_lock(&clients_mutex);
            int r_idx = find_room_by_name(room_name);
            if (r_idx == -1 && room_count < MAX_ROOMS) {
                r_idx = room_count++;
                strcpy(rooms[r_idx].name, room_name);
                rooms[r_idx].member_count = 0;
            }
            if (r_idx != -1) {
                if (room_has_member(r_idx, clients[client_idx].username) == -1) {
                    strcpy(rooms[r_idx].members[rooms[r_idx].member_count++], clients[client_idx].username);
                }
                char resp[128];
                snprintf(resp, sizeof(resp), "OK JOINED %s", room_name);
                send_response(sock, resp);
                snprintf(log_buf, sizeof(log_buf), "%s joined room %s", clients[client_idx].username, room_name);
                log_event("JOIN", log_buf);
            } else {
                send_response(sock, "ERR 009 ROOM_LIMIT_REACHED");
            }
            pthread_mutex_unlock(&clients_mutex);
        } else if (strncmp(buffer, "LEAVE ", 6) == 0) {
            char room_name[64] = "";
            sscanf(buffer + 6, "%63s", room_name);
            pthread_mutex_lock(&clients_mutex);
            int r_idx = find_room_by_name(room_name);
            int m_idx = (r_idx == -1) ? -1 : room_has_member(r_idx, clients[client_idx].username);
            if (r_idx == -1 || m_idx == -1) {
                send_response(sock, "ERR 003 ROOM_NOT_FOUND");
            } else {
                for (int k = m_idx; k < rooms[r_idx].member_count - 1; k++) {
                    strcpy(rooms[r_idx].members[k], rooms[r_idx].members[k + 1]);
                }
                rooms[r_idx].member_count--;
                char resp[128];
                snprintf(resp, sizeof(resp), "OK LEFT %s", room_name);
                send_response(sock, resp);
                snprintf(log_buf, sizeof(log_buf), "%s left room %s", clients[client_idx].username, room_name);
                log_event("LEAVE", log_buf);
            }
            pthread_mutex_unlock(&clients_mutex);
        } else if (strcmp(buffer, "ROOMS") == 0) {
            pthread_mutex_lock(&clients_mutex);
            char r_resp[OUT_SIZE] = "OK ROOMS ";
            for (int i = 0; i < room_count; i++) {
                if (i > 0) strcat(r_resp, ",");
                strcat(r_resp, rooms[i].name);
            }
            send_response(sock, r_resp);
            pthread_mutex_unlock(&clients_mutex);
        } else if (strncmp(buffer, "RMSG ", 5) == 0) {
            char room_name[64], msg[BUFFER_SIZE];
            if (sscanf(buffer + 5, "%63s %[^\n]", room_name, msg) == 2) {
                pthread_mutex_lock(&clients_mutex);
                int r_idx = find_room_by_name(room_name);
                if (r_idx != -1) {
                    send_response(sock, "OK SENT");
                    char rmsg[OUT_SIZE];
                    snprintf(rmsg, sizeof(rmsg), "MSG ROOM %s %s %s", room_name, clients[client_idx].username, msg);
                    for (int m = 0; m < rooms[r_idx].member_count; m++) {
                        int target_idx = find_client_by_name(rooms[r_idx].members[m]);
                        if (target_idx != -1 && target_idx != client_idx) {
                            forward_message(clients[target_idx].socket, rmsg);
                        }
                    }
                    snprintf(log_buf, sizeof(log_buf), "%s -> room %s: %s", clients[client_idx].username, room_name, msg);
                    log_event("RMSG", log_buf);
                } else {
                    send_response(sock, "ERR 003 ROOM_NOT_FOUND");
                }
                pthread_mutex_unlock(&clients_mutex);
            } else {
                send_response(sock, "ERR 007 BAD_FORMAT");
            }
        } else if (strncmp(buffer, "SENDFILE ", 9) == 0) {
            char target[64], filename_raw[128];
            long filesize = 0;
            if (sscanf(buffer + 9, "%63s %127s %ld", target, filename_raw, &filesize) != 3 || filesize < 0) {
                send_response(sock, "ERR 007 BAD_FORMAT");
                continue;
            }
            const char *filename = safe_basename(filename_raw);
            if (filename[0] == '\0' || strcmp(filename, "..") == 0) {
                send_response(sock, "ERR 007 BAD_FORMAT");
                continue;
            }

            /* Always consume exactly <filesize> bytes so the stream stays in sync.
               Only keep them in memory if the size is acceptable. */
            int too_large = (filesize > MAX_FILE_SIZE);
            char *data = too_large ? NULL : malloc(filesize > 0 ? filesize : 1);
            char file_buf[1024];
            long remaining = filesize, got = 0;
            int conn_lost = 0;
            while (remaining > 0) {
                size_t to_read = (size_t)remaining < sizeof(file_buf) ? (size_t)remaining : sizeof(file_buf);
                ssize_t bytes = recv(sock, file_buf, to_read, 0);
                if (bytes <= 0) { conn_lost = 1; break; }
                if (data) memcpy(data + got, file_buf, bytes);
                got += bytes;
                remaining -= bytes;
            }
            if (conn_lost) { free(data); break; }   /* client died mid-transfer */

            if (too_large || !data) {
                free(data);
                send_response(sock, "ERR 004 FILE_TOO_LARGE");
                continue;
            }

            pthread_mutex_lock(&clients_mutex);
            int target_idx = find_client_by_name(target);
            int r_idx = (target_idx == -1) ? find_room_by_name(target) : -1;
            if (target_idx == -1 && r_idx == -1) {
                /* user unknown -> 002 ; (a room name that does not exist is also unknown) */
                send_response(sock, "ERR 002 USER_NOT_FOUND");
                pthread_mutex_unlock(&clients_mutex);
                free(data);
                continue;
            }

            /* 1. store a copy under ./storage/<regno>/<sender>/<filename> */
            char save_dir[256];
            snprintf(save_dir, sizeof(save_dir), "%s/%s", STORAGE_BASE, clients[client_idx].username);
            create_dir_if_not_exists(save_dir);
            char filepath[512];
            snprintf(filepath, sizeof(filepath), "%s/%s", save_dir, filename);
            FILE *fp = fopen(filepath, "wb");
            if (fp) { fwrite(data, 1, filesize, fp); fclose(fp); }

            /* 2. forward header + raw bytes to the receiver(s) */
            char header[OUT_SIZE];
            snprintf(header, sizeof(header), "MSG FILE %s %s %ld\n", clients[client_idx].username, filename, filesize);
            if (target_idx != -1) {
                send_all(clients[target_idx].socket, header, strlen(header));
                send_all(clients[target_idx].socket, data, filesize);
            } else {
                for (int m = 0; m < rooms[r_idx].member_count; m++) {
                    int t = find_client_by_name(rooms[r_idx].members[m]);
                    if (t != -1 && t != client_idx) {
                        send_all(clients[t].socket, header, strlen(header));
                        send_all(clients[t].socket, data, filesize);
                    }
                }
            }
            free(data);

            char ok_resp[256];
            snprintf(ok_resp, sizeof(ok_resp), "OK FILE_RECEIVED %s", filename);
            send_response(sock, ok_resp);
            snprintf(log_buf, sizeof(log_buf), "File %s (%ld bytes) from %s to %s stored at %s",
                     filename, filesize, clients[client_idx].username, target, filepath);
            log_event("SENDFILE", log_buf);
            pthread_mutex_unlock(&clients_mutex);
        } else if (strcmp(buffer, "QUIT") == 0) {
            send_response(sock, "OK BYE");
            break;
        } else {
            send_response(sock, "ERR 006 UNKNOWN_COMMAND");
        }
    }

    pthread_mutex_lock(&clients_mutex);
    if (clients[client_idx].registered) {
        snprintf(log_buf, sizeof(log_buf), "Client disconnected: %s", clients[client_idx].username);
        log_event("DISCONNECT", log_buf);

        char leave_msg[128];
        snprintf(leave_msg, sizeof(leave_msg), "MSG SYS User %s left", clients[client_idx].username);
        remove_client_from_rooms(clients[client_idx].username);
        clients[client_idx].registered = 0;      /* so the leave notice is not echoed to self */
        broadcast_presence(leave_msg, sock);
    } else {
        log_event("DISCONNECT", "Unregistered client disconnected");
    }
    close(sock);
    clients[client_idx].socket = 0;
    clients[client_idx].registered = 0;
    clients[client_idx].username[0] = '\0';
    pthread_mutex_unlock(&clients_mutex);

    return NULL;
}

int main() {
    signal(SIGPIPE, SIG_IGN);   /* a write to a dead client must not kill the server */
    int server_fd;
    struct sockaddr_in address;
    int opt = 1;

    for (int i = 0; i < MAX_CLIENTS; i++) {
        clients[i].socket = 0;
        clients[i].registered = 0;
    }

    create_dir_if_not_exists(STORAGE_BASE);

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 10) < 0) {
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }

    printf("NetMessenger Server running on port %d [NID: 7027]...\n", PORT);
    log_event("SERVER_START", "Server started operating successfully");

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t addrlen = sizeof(client_addr);
        int new_socket = accept(server_fd, (struct sockaddr *)&client_addr, &addrlen);

        if (new_socket < 0) continue;

        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, ip, sizeof(ip));
        char conn_msg[128];
        snprintf(conn_msg, sizeof(conn_msg), "Connection from %s:%d", ip, ntohs(client_addr.sin_port));
        log_event("CONNECT", conn_msg);

        pthread_mutex_lock(&clients_mutex);
        int slot = -1;
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].socket == 0) {
                slot = i;
                break;
            }
        }

        if (slot != -1) {
            clients[slot].socket = new_socket;
            pthread_t thread_id;
            int *arg = malloc(sizeof(int));
            *arg = slot;
            pthread_create(&thread_id, NULL, handle_client, arg);
            pthread_detach(thread_id);
        } else {
            close(new_socket);
        }
        pthread_mutex_unlock(&clients_mutex);
    }

    return 0;
}