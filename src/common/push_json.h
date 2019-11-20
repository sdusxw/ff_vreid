#include <iostream>
#include <string>
#include <curl/curl.h>

using std::string;

//JpgPusher  上传图片识别结果到云平台数据汇聚接口
class JsonPusher
{
public:
    bool initialize();
    string push_json(string url, string json);

private:
    pthread_mutex_t mutex_lock;        //互斥锁
};
