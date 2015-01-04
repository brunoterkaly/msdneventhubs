#include "proton/message.h"
#include "proton/messenger.h"

#include "pncompat/misc_funcs.inc"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <dirent.h>

//#define MYDEBUG

char msgbuffer [2048];

char fields[13][512];



int waitms(int millisecs) 
{
    fd_set dummy;
    struct timeval toWait;
    FD_ZERO(&dummy);
    toWait.tv_sec = millisecs / 1000;
    toWait.tv_usec = (millisecs % 1000) * 1000;
    return select(0, &dummy, NULL, NULL, &toWait);
}

#define check(messenger)                                                     \
  {                                                                          \
    if(pn_messenger_errno(messenger))                                        \
    {                                                                        \
      printf("check\n");                                                     \
      die(__FILE__, __LINE__, pn_error_text(pn_messenger_error(messenger))); \
    }                                                                        \
  }  


void die(const char *file, int line, const char *message)
{
  printf("Dead\n");
  fprintf(stderr, "%s:%i: %s\n", file, line, message);
  exit(1);
}

int sendMessage(pn_messenger_t * messenger, int month, char *f1, char *f2) 
{

    // string from portal
    // Endpoint=sb://brunoeventhub-ns.servicebus.windows.net/;SharedAccessKeyName=SendRule;
    // SharedAccessKey=V0Plw5UIRzpwg16tNXwpbBA/LU6qAX3t54YXjIQYgy8=
    // SharedAccessKey=3steatCSObflHQrOo9rjdG6DEvxT29cKZb8MIdfxBA4=
    // SharedAccessKey=3steatCSObflHQrOo9rjdG6DEvxT29cKZb8MIdfxBA4%3D

    char * address = (char *) "amqps://SendRule:3steatCSObflHQrOo9rjdG6DEvxT29cKZb8MIdfxBA4%3D@temperatureeventhub-ns.servicebus.windows.net/temperatureeventhub";


    int n = sprintf (msgbuffer, "%s,%d,%s", f1, month, f2);
    pn_message_t * message;
    pn_data_t * body;
    message = pn_message();
    pn_message_set_address(message, address);
    pn_message_set_content_type(message, (char*) "application/octect-stream");
    pn_message_set_inferred(message, true);

    body = pn_message_body(message);
    pn_data_put_binary(body, pn_bytes(strlen(msgbuffer), msgbuffer));

    pn_messenger_put(messenger, message);
    check(messenger);
    pn_messenger_send(messenger, 1);
    check(messenger);

    pn_message_free(message);
}



int main(int argc, char** argv) 
{
    printf("Press Ctrl-C to stop the sender process\n");
    FILE * fp;
    char  line[512];
    size_t len = 0;
    size_t read = 0;
    int i = 0;
    int curr_field = 0;
    int trg_col = 0;
    pn_messenger_t *messenger = pn_messenger(NULL);
    pn_messenger_set_outgoing_window(messenger, 1);
    pn_messenger_start(messenger);

    fp = fopen("weatherdata.csv", "r");
    if (fp == NULL)
      exit(EXIT_FAILURE);

    while (fgets(line, 512, fp)!=NULL)
    {
	for (i = 0; line[i] != '\0'; i++)
	{
		// if hit comma, finished a field
		if (line[i] == ',')
		{
			fields[curr_field][trg_col] = '\0';
			//printf("Finished a field, trg_col = %d\n", trg_col);
			//printf("%s\n", fields[curr_field]);
			trg_col = 0;
			curr_field += 1;
		}
		else
		{
			fields[curr_field][trg_col] = line[i];
			trg_col += 1;

		}
        }
        trg_col = 0;
        curr_field = 0;
        for (i = 1; i < 13; i++)
	{
	    sendMessage(messenger, i, fields[0], fields[i]);
            printf("%s -> %s\n", fields[0], fields[i]);
	}

        printf("\n");
    }
    fclose(fp);

    exit(-1);

    list_files("/home/azureuser/dev", 0, messenger);

    // release messenger resources
    pn_messenger_stop(messenger);
    pn_messenger_free(messenger);

    return 0;
}
