#include "pull_jpg2ram.h"

#include <string.h>
#include <memory.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <iostream>
#include <string>

#include <openssl/err.h>

#define MUTEX_TYPE       pthread_mutex_t
#define MUTEX_SETUP(x)   pthread_mutex_init(&(x), NULL)
#define MUTEX_CLEANUP(x) pthread_mutex_destroy(&(x))
#define MUTEX_LOCK(x)    pthread_mutex_lock(&(x))
#define MUTEX_UNLOCK(x)  pthread_mutex_unlock(&(x))
#define THREAD_ID        pthread_self()

void handle_error(const char *file, int lineno, const char *msg)
{
    fprintf(stderr, "** %s:%d %s\n", file, lineno, msg);
    //ERR_print_errors_fp(stderr);
    /* exit(-1); */
}

/* This array will store all of the mutexes available to OpenSSL. */
static MUTEX_TYPE *mutex_buf = NULL;

static void locking_function(int mode, int n, const char *file, int line)
{
    if(mode & CRYPTO_LOCK)
        MUTEX_LOCK(mutex_buf[n]);
    else
        MUTEX_UNLOCK(mutex_buf[n]);
}

static unsigned long id_function(void)
{
    return ((unsigned long)THREAD_ID);
}

int thread_setup(void)
{
    int i;
    
    mutex_buf = (pthread_mutex_t*)malloc(CRYPTO_num_locks() * sizeof(MUTEX_TYPE));
    if(!mutex_buf)
        return 0;
    for(i = 0;  i < CRYPTO_num_locks();  i++)
        MUTEX_SETUP(mutex_buf[i]);
    CRYPTO_set_id_callback(id_function);
    CRYPTO_set_locking_callback(locking_function);
    return 1;
}

int thread_cleanup(void)
{
    int i;
    
    if(!mutex_buf)
        return 0;
    CRYPTO_set_id_callback(NULL);
    CRYPTO_set_locking_callback(NULL);
    for(i = 0;  i < CRYPTO_num_locks();  i++)
        MUTEX_CLEANUP(mutex_buf[i]);
    free(mutex_buf);
    mutex_buf = NULL;
    return 1;
}

//CURL函数的写内存回调函数
static size_t WriteMemoryCallback(void *contents, size_t size, size_t nmemb,
                                  void *userp)
{
    size_t realsize = size * nmemb;
    struct MemoryStruct *mem = (struct MemoryStruct *) userp;
    
    mem->memory = (char*) realloc(mem->memory, mem->size + realsize + 1);
    if (mem->memory == NULL)
    {
        /* out of memory! */
        printf("not enough memory (realloc returned NULL)\n");
        return 0;
    }
    
    memcpy(&(mem->memory[mem->size]), contents, realsize);
    mem->size += realsize;
    mem->memory[mem->size] = 0;
    
    return realsize;
}
/*********************类JpgPuller实现************************************/
bool JpgPuller::initialize()
{
    pthread_mutex_init(&mutex_lock, NULL);
    p_jpg_image = (char*) malloc(2000000);    //jpg图像大小不超过2M
    if (p_jpg_image)
    return true;
    else
    return false;
}
bool JpgPuller::pull_image(char * url)
{
    bool result = false;
    char char_msg[100] = "";
    std::string log_msg = "";
    pthread_mutex_lock(&mutex_lock);
    CURL *curl_handle;
    CURLcode res;
    chunk.memory = (char*) malloc(1); /* will be grown as needed by the realloc above */
    chunk.size = 0; /* no data at this point */
    jpg_size = 0;
    
    /* init the curl session */
    curl_handle = curl_easy_init();
    curl_easy_setopt(curl_handle, CURLOPT_NOSIGNAL,1L);
    /* specify URL to get */
    char ipc_url[1024] =
    { 0 };
    sprintf(ipc_url, "%s", url);
    curl_easy_setopt(curl_handle, CURLOPT_URL, ipc_url);
    
    /* send all data to this function  */
    curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION,
                     WriteMemoryCallback);
    
    /* we pass our 'chunk' struct to the callback function */
    curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void * )&chunk);
    
    /* multi thread safe */
    curl_easy_setopt(curl_handle, CURLOPT_NOSIGNAL, 1L);
    
    curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, 1);
    
    /* some servers don't like requests that are made without a user-agent
     field, so we provide one */
    curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "libcurl-agent/1.0");
    
    /* get it! */
    res = curl_easy_perform(curl_handle);
    
    /* check for errors */
    if (res != CURLE_OK)
    {
        sprintf(char_msg, "curl_easy_perform() failed: %s\n",
                curl_easy_strerror(res));
    }
    else
    {
        /*
         * Now, our chunk.memory points to a memory block that is chunk.size
         * bytes big and contains the remote file.
         *
         * Do something nice with it!
         */
        if (chunk.size != 0)
        {
            jpg_size = chunk.size;
            memcpy(p_jpg_image, chunk.memory, chunk.size);
            result = true;
        }
    }
    /* cleanup curl stuff */
    curl_easy_cleanup(curl_handle);
    if (chunk.memory)
    free(chunk.memory);
    pthread_mutex_unlock(&mutex_lock);
    if (!result)
    {
        sprintf(char_msg, "get_snapshot failed: %s\n", url);
    }
    return result;
}

void JpgPuller::free_memory()
{
    if (p_jpg_image)
    {
        free(p_jpg_image);
        p_jpg_image = nullptr;
    }
}
/***************************end****************************************/
