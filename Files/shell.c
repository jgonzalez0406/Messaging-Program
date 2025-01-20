/* shell.c
 * Demonstration of multiplexing I/O with a background thread also printing.
 **/

#include "smq/thread.h"
#include "smq/client.h"
#include "smq/utils.h"
#include "smq/queue.h"

#include <ctype.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <semaphore.h>

#include <termios.h>
#include <unistd.h>

/* Constants */
sem_t Shutdown;
const size_t NMESSAGES = 1<<16;
#define BACKSPACE   127

char *host = "student12.cse.nd.edu"; // default values
char *port = "9002";
char *name = "Tester";

void usage(int status) {
    fprintf(stderr, "Usage: ./shell [options]\n");
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "    -h		   Show help and usage\n");
	fprintf(stderr, "    -s        host\n");
	fprintf(stderr, "    -p        port\n");
	fprintf(stderr, "    -n        name\n");
    exit(status);

}

/* Functions
 * https://viewsourcecode.org/snaptoken/kilo/02.enteringRawMode.html
 */

void toggle_raw_mode() {
    static struct termios OriginalTermios = {0};
    static bool enabled = false;

    if (enabled) {
    	tcsetattr(STDIN_FILENO, TCSAFLUSH, &OriginalTermios);
    } else {
	tcgetattr(STDIN_FILENO, &OriginalTermios);

	atexit(toggle_raw_mode);

	struct termios raw = OriginalTermios;
	raw.c_lflag &= ~(ECHO | ICANON | IEXTEN);
	raw.c_cc[VMIN] = 1;
	raw.c_cc[VTIME] = 0;

	tcsetattr(STDIN_FILENO, TCSAFLUSH, &raw);
    enabled = true;
    }
}

/* Threads */

void *incoming_thread(void *arg) {
    SMQ *smq = (SMQ *)arg;
    size_t messages = 0;

    while (smq_running(smq)) {
        char *message = smq_retrieve(smq);
        if (message) {
			printf("\r%s > %-80s\n", smq->name, message);
            free(message);
            messages++;
        }

        if (messages == NMESSAGES) {
            sem_post(&Shutdown);
        }
    }

    return NULL;
}

void * outgoing_thread(void *arg)
{
    SMQ *smq = (SMQ *)arg;
    char input_buffer[BUFSIZ] = "";
    size_t input_index = 0;

    while (smq_running(smq)) {
        char input_char = 0;
        
        read(STDIN_FILENO, &input_char, 1);

        if (input_char == '\n') {
            if (strcmp(input_buffer, "/quit") == 0 || strcmp(input_buffer, "/exit") == 0) {
                smq_shutdown(smq);
                break;
            } else if (strlen(input_buffer) > 0) {
                smq_publish(smq, "shell", input_buffer);
                printf("\r%s > %-80s", name, input_buffer);  // Print the message locally, then move to the next line
                input_index = 0;
                input_buffer[0] = 0;
            }

        } else if (input_char == BACKSPACE && input_index > 0) {
            input_index--;
            input_buffer[input_index] = '\0';
            // Move cursor back, overwrite the character, and erase the rest of the line
            printf("\r%s > %s \r", name, input_buffer);
            fflush(stdout);
        } else if (!iscntrl(input_char) && input_index < BUFSIZ - 1) {
            input_buffer[input_index++] = input_char;
            input_buffer[input_index] = '\0';
            printf("\r%s > %s", name, input_buffer);  // Reprint the input line without a newline
            fflush(stdout);
        }
    }
    return NULL;
}

/* Main Execution */

int main(int argc, char *argv[]) {
    toggle_raw_mode();

	int argind = 1;
	while (argind < argc && strlen(argv[argind]) > 1 && argv[argind][0] == '-') {
		char *arg = argv[argind++];
		if (streq(arg, "-h")) {
			usage(EXIT_SUCCESS);
		} else if (streq(arg, "-s")) {
			host = argv[argind++];
		} else if (streq(arg, "-p")) {
			port = argv[argind++];
		} else if (streq(arg, "-n")) {
			name = argv[argind++];
		} else {
			usage(EXIT_FAILURE);
		}
	}

    /* Initialize semaphore */
    sem_init(&Shutdown, 0, 0);

	/* Create and start message queue */
    SMQ *smq = smq_create(name, host, port);

    // Subscribe to the shell topic
    smq_subscribe(smq, "shell");

    /* Background Thread */
	Thread incoming;
    Thread outgoing;
    thread_create(&incoming, NULL, incoming_thread, smq);
    thread_create(&outgoing, NULL, outgoing_thread, smq);

    // Welcome message
	printf("Welcome to the Simple Message Queue (SMQ) Shell, %s!\n", name);
	printf("You are connected to Server: %s:%s\n", host, port);

    // Join the threads
    thread_join(incoming, NULL);
    smq_delete(smq);
    thread_join(outgoing, NULL);

	return 0;
}
