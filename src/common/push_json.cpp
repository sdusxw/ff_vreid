#include "push_json.h"

#include <string.h>
#include <memory.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <iostream>
#include <string>

struct MemoryStructX {
    char *memory;
    size_t size;
};

static size_t
WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
    size_t realsize = size * nmemb;
    struct MemoryStructX *mem = (struct MemoryStructX *)userp;
    
    char *ptr = (char*)realloc(mem->memory, mem->size + realsize + 1);
    if(!ptr) {
        /* out of memory! */
        printf("not enough memory (realloc returned NULL)\n");
        return 0;
    }
    
    mem->memory = ptr;
    memcpy(&(mem->memory[mem->size]), contents, realsize);
    mem->size += realsize;
    mem->memory[mem->size] = 0;
    
    return realsize;
}

/*********************类JpgPusher实现************************************/
bool JsonPusher::initialize()
{
    pthread_mutex_init(&mutex_lock, NULL);
    return true;
}
string JsonPusher::push_json(string url, string json)
{
    string result="";
    pthread_mutex_lock(&mutex_lock);
    
    
    CURL *curl;
    CURLcode res;
    
    struct MemoryStructX chunk;
    
    chunk.memory = (char*)malloc(1);  /* will be grown as needed by realloc above */
    chunk.size = 0;    /* no data at this point */
    
    struct curl_httppost *formpost = NULL;
    struct curl_httppost *lastptr = NULL;
    struct curl_slist *headerlist = NULL;
    static const char buf[] = "Expect:";
    
    /* Fill in the submit field too, even if this is rarely needed */
    
    
    curl = curl_easy_init();
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL,1L);
    /* initialize custom header list (stating that Expect: 100-continue is not
     wanted */
    headerlist = curl_slist_append(headerlist, buf);
    
    headerlist = curl_slist_append(headerlist, "Accept: application/json");
    headerlist = curl_slist_append(headerlist, "Content-Type: application/json");
    headerlist = curl_slist_append(headerlist, "charsets: utf-8");
    
    /* send all data to this function  */
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
    
    /* we pass our 'chunk' struct to the callback function */
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&chunk);
    
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "libcurl-agent/1.0");
    if(curl) {
        /* what URL that receives this POST */
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        
        /* ask libcurl to show us the verbose output */
        //curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
        
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headerlist);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json.c_str());
        
        
        /* Perform the request, res will get the return code */
        res = curl_easy_perform(curl);
        /* Check for errors */
        if(res != CURLE_OK)
            fprintf(stderr, "curl_easy_perform() failed: %s\n",
                    curl_easy_strerror(res));
        
    
        string str(chunk.memory, chunk.size);
        result = str;
        if(chunk.memory){
            free(chunk.memory);
            chunk.memory = nullptr;
        }
        /* always cleanup */
        curl_easy_cleanup(curl);
        
        /* then cleanup the formpost chain */
        curl_formfree(formpost);
        /* free slist */
        curl_slist_free_all(headerlist);
    }
    pthread_mutex_unlock(&mutex_lock);
    return result;
}
/***************************end****************************************/
