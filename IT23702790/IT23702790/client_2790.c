#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>
#include <sys/stat.h>

#define PORT 8790
#define BUFFER_SIZE 2048

int sock = 0;

/* read exactly n bytes from the socket (recv may return fewer per call) */
int recv_exact(char *dst, long n) {
    long got = 0;
    while (got < n) {
        ssize_t r = recv(sock, dst + got, n - got, 0);
        if (r <= 0) return -1;
        got += r;
    }
    return 0;
}

void *receive_handler(void *arg) {
    (void)arg;
    char buffer[BUFFER_SIZE];
    while (1) {
        size_t bytes_read = 0;
        while (bytes_read < sizeof(buffer) - 1) {
            char c;
            ssize_t res = recv(sock, &c, 1, 0);
            if (res <= 0) {
                printf("\nDisconnected from server.\n");
                exit(0);
            }
            if (c == '\n') break;
            buffer[bytes_read++] = c;
        }
        buffer[bytes_read] = '\0';

        /* incoming file: "MSG FILE <sender> <filename> <size>" + <size> raw bytes */
        char sender[64], fname[128];
        long fsize;
        if (sscanf(buffer, "MSG FILE %63s %127s %ld", sender, fname, &fsize) == 3 && fsize >= 0) {
            char *data = malloc(fsize > 0 ? fsize : 1);
            if (!data || recv_exact(data, fsize) < 0) {
                printf("\nDisconnected during file transfer.\n");
                exit(0);
            }
            mkdir("received", 0777);
            char path[256];
            snprintf(path, sizeof(path), "received/%s", fname);
            FILE *fp = fopen(path, "wb");
            if (fp) { fwrite(data, 1, fsize, fp); fclose(fp); }
            free(data);
            printf("\n[FILE] Received '%s' (%ld bytes) from %s -> saved to %s\n> ", fname, fsize, sender, path);
            fflush(stdout);
            continue;
        }

        printf("\n[SERVER] %s\n> ", buffer);
        fflush(stdout);
    }
    return NULL;
}

void send_file_command(const char *target, const char *filepath) {
    FILE *fp = fopen(filepath, "rb");
    if (!fp) {
        printf("Error: Cannot open local file %s\n", filepath);
        return;
    }

    fseek(fp, 0, SEEK_END);
    long filesize = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    const char *filename = strrchr(filepath, '/');
    if (!filename) filename = filepath;
    else filename++;

    char cmd[512];
    snprintf(cmd, sizeof(cmd), "SENDFILE %s %s %ld\n", target, filename, filesize);
    send(sock, cmd, strlen(cmd), MSG_NOSIGNAL);

    char buffer[1024];
    size_t bytes_read;
    while ((bytes_read = fread(buffer, 1, sizeof(buffer), fp)) > 0) {
        send(sock, buffer, bytes_read, MSG_NOSIGNAL);
    }

    fclose(fp);
    printf("File %s sent successfully.\n", filename);
}

int main(int argc, char *argv[]) {
    const char *host = (argc > 1) ? argv[1] : "127.0.0.1";
    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    char buffer[BUFFER_SIZE];

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("\n Socket creation error \n");
        return -1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);

    if (inet_pton(AF_INET, host, &serv_addr.sin_addr) <= 0) {
        printf("\nInvalid address/ Address not supported \n");
        return -1;
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("\nConnection Failed");
        return -1;
    }

    printf("Connected to NetMessenger Server %s:%d.\n", host, PORT);

    pthread_t recv_thread;
    pthread_create(&recv_thread, NULL, receive_handler, NULL);

    while (1) {
        printf("> ");
        fflush(stdout);
        if (!fgets(buffer, sizeof(buffer), stdin)) break;

        buffer[strcspn(buffer, "\n")] = 0;

        if (strncmp(buffer, "SENDFILE ", 9) == 0) {
            char target[64], filepath[256];
            if (sscanf(buffer + 9, "%s %s", target, filepath) == 2) {
                send_file_command(target, filepath);
            } else {
                printf("Usage: SENDFILE <target> <filepath>\n");
            }
        } else {
            strcat(buffer, "\n");
            send(sock, buffer, strlen(buffer), MSG_NOSIGNAL);
        }

        if (strcmp(buffer, "QUIT\n") == 0) {
            sleep(1);
            break;
        }
    }

    close(sock);
    return 0;
}