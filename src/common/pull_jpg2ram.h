#include <curl/curl.h>

int thread_setup(void);
int thread_cleanup(void);

//JpgPuller类的内存结构体
struct MemoryStruct
{
    char *memory;
    size_t size;
};
//JpgPuller抓图类
class JpgPuller
{
    public:
    JpgPuller()
    {
        p_jpg_image = NULL;
        jpg_size = 0;
    }    //构造函数
    bool initialize();                    //分配空间
    bool pull_image(char * url);        //抓拍一张图
    void free_memory();                 //释放分配的图片
    char * p_jpg_image;                //Jpg图像指针
    size_t jpg_size;                    //jpg图像大小
    private:
    struct MemoryStruct chunk;        //内存结构体
    pthread_mutex_t mutex_lock;        //互斥锁
};
