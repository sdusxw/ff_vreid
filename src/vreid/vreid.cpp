//
//  simplesvr.cc
//
//  Copyright (c) 2013 Yuji Hirose. All rights reserved.
//  The Boost Software License 1.0
//

#include <cstdio>
#include <iostream>
#include <fstream>
#include <json/json.h>
#include <time.h>
#include <boost/thread.hpp>
#include <stdio.h>
#include <stdlib.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/sem.h>
#include <sys/types.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>
#include <sched.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <string>

#include "cppkafka/utils/buffered_producer.h"
#include "cppkafka/configuration.h"

#include "lpr_alg.h"
#include "common.h"
#include "httplib.h"
#include "concurrent_queue.h"
#include "pull_jpg2ram.h"
#include "push_json.h"

using namespace httplib;
using namespace std;

using cppkafka::BufferedProducer;
using cppkafka::Configuration;
using cppkafka::Topic;
using cppkafka::MessageBuilder;
using cppkafka::Message;

#define MAX_BUFF_LENGTH     (10*1024*1024)  //共享内存大小10M

class HttpInfo
{
public:
    Request req;
    Response res;
};

class FileInfo
{
public:
    char * p;
    int len;
    string filename;
    string lpr_res_json;
};

ServerConf server_conf;
PVPR pvpr;

concurrent_queue<HttpInfo> g_http_buff;

boost::thread thread_http_handler;

pthread_mutex_t mutex_;

void task_http_handler();

concurrent_queue<FileInfo> g_file_queue;

concurrent_queue<string> g_kafka_log_queue;

boost::thread thread_save_file;

boost::thread thread_kafka_log;

void task_save_file();

void task_kafka_log();

string dump_headers(const Headers &headers) {
    string s;
    char buf[BUFSIZ];
    
    for (const auto &x : headers) {
        snprintf(buf, sizeof(buf), "%s: %s\n", x.first.c_str(), x.second.c_str());
        s += buf;
    }
    
    return s;
}

void save_file(const char * file_content, int len, string file_name, string lpr_res_json)
{
    string path="",file="";
    if (split_filename(file_name, path, file)) {
        path = server_conf.image_base + "/" + path;
        create_dir(path.c_str());
    }
    file_name = server_conf.image_base + "/" + file_name;
    ofstream output( file_name, ios::out | ios::binary );
    if( ! output )
    {
        cerr << "Open output file error!" << endl;
        if (file_content) {
            free((void*)file_content);
            file_content = NULL;
        }
        return;
    }
    
    output.write (file_content, len);
    
    output.close();
    if (file_content) {
        free((void*)file_content);
        file_content = NULL;
    }
    //保存识别结果json文件
    string json_name = file_name + ".json";
    ofstream json_output( json_name, ios::out | ios::binary );
    if( ! json_output )
    {
        cerr << "Open json_output file error!" << endl;
        return;
    }
    
    json_output.write (lpr_res_json.c_str(), lpr_res_json.length());
    
    json_output.close();
}

string dump_multipart_files(const MultipartFiles &files) {
    string s;
    char buf[BUFSIZ];
    
    s += "--------------------------------\n";
    
    for (const auto &x : files) {
        const auto &name = x.first;
        const auto &file = x.second;
        
        snprintf(buf, sizeof(buf), "name: %s\n", name.c_str());
        s += buf;
        
        snprintf(buf, sizeof(buf), "filename: %s\n", file.filename.c_str());
        s += buf;
        
        snprintf(buf, sizeof(buf), "content type: %s\n", file.content_type.c_str());
        s += buf;
        
        snprintf(buf, sizeof(buf), "text offset: %lu\n", file.offset);
        s += buf;
        
        snprintf(buf, sizeof(buf), "text length: %lu\n", file.length);
        s += buf;
        
        s += "----------------\n";
    }
    
    return s;
}

string log(const Request &req, const Response &res) {
    string s;
    char buf[BUFSIZ];
    
    s += "================================\n";
    
    snprintf(buf, sizeof(buf), "%s %s %s", req.method.c_str(),
             req.version.c_str(), req.path.c_str());
    s += buf;
    
    string query;
    for (auto it = req.params.begin(); it != req.params.end(); ++it) {
        const auto &x = *it;
        snprintf(buf, sizeof(buf), "%c%s=%s",
                 (it == req.params.begin()) ? '?' : '&', x.first.c_str(),
                 x.second.c_str());
        query += buf;
    }
    snprintf(buf, sizeof(buf), "%s\n", query.c_str());
    s += buf;
    
    s += dump_headers(req.headers);
    s += dump_multipart_files(req.files);
    
    s += "--------------------------------\n";
    
    snprintf(buf, sizeof(buf), "%d\n", res.status);
    s += buf;
    s += dump_headers(res.headers);
    
    return s;
}

void task_http_handler()
{
    while (true) {
        HttpInfo http_info;
        g_http_buff.wait_and_pop(http_info);
        auto body = "{\"code\":0}";
        http_info.res.set_content(body, "application/json");
    }
}

void task_save_file()
{
    while (true) {
        FileInfo file_info;
        g_file_queue.wait_and_pop(file_info);
        save_file((const char*)file_info.p, file_info.len, file_info.filename, file_info.lpr_res_json);
    }
}

void task_kafka_log()
{
    string brokers;
    string topic_name;
    
    brokers = "172.31.3.1:9092,172.31.3.2:9092,172.31.3.3:9092";
    topic_name = "picturelog";
    
    // Create a message builder for this topic
    MessageBuilder builder(topic_name);
    
    // Construct the configuration
    Configuration config = {
        { "metadata.broker.list", brokers }
    };
    
    // Create the producer
    BufferedProducer<string> producer(config);
    
    // Set a produce success callback
    producer.set_produce_success_callback([](const Message& msg) {
        //cout << "Successfully produced message with payload " << msg.get_payload() << endl;
    });
    // Set a produce failure callback
    producer.set_produce_failure_callback([](const Message& msg) {
        cout << "Failed to produce message with payload " << msg.get_payload() << endl;
        // Return false so we stop trying to produce this message
        return false;
    });
    
    cout << "Producing messages into topic " << topic_name << endl;
    
    while (true) {
        string log_msg;
        g_kafka_log_queue.wait_and_pop(log_msg);
        cout << log_msg << endl;
        // Set the payload on this builder
        builder.payload(log_msg);
        
        // Add the message we've built to the buffered producer
        producer.add_message(builder);
        // Flush all produced messages
        producer.flush();
    }
}

int main(int argc, const char **argv) {
    //算法库初始化
    if(!vlpr_init())
    {
        cout<<"LPR_ALG init Fail!"<<endl;
        exit(-1);
    }else{
        cout<<"LPR_ALG init OK!"<<endl;
    }
    
    pvpr=(PVPR)malloc(sizeof(VPR));
    
    pthread_mutex_init(&mutex_,NULL);
    
    //读取服务器配置文件
    server_conf.server_ip="0.0.0.0";
    server_conf.server_port=80;
    server_conf.image_base="/data";
    if(read_config(server_conf))
    {
        cout << "Read Server Configure OK!" << endl;
    }else{
        cout << "Read Server Configure Fail!" << endl;
    }
    Server svr;
    svr.Post("/vreid", [](const Request &req, Response &res) {
        pthread_mutex_lock(&mutex_);
        //1. curl https://
        long res_receivetime=get_unix_ts_ms();//接收图片时间
        string res_passId;
        string res_path;
        string res_plateNo="E";
        string res_plateColor="0";
        Json::Value res_json_rect;
        
        std::string msg_jpg = req.body;
        Json::Reader reader;
        Json::Value json_object;
        
        if (!reader.parse(msg_jpg, json_object))
        {
            //JSON格式错误导致解析失败
            cout << "[json]Kafka Topic2解析失败" << endl;
        }else{
            //kafka日志
            Json::Value json_kafka_log;
            json_kafka_log["type"]="sjk-beichuang-lpa";
            //处理kafka的Topic2消息
            string string_img_url = json_object["imgURL"].asString();
            res_passId = json_object["passId"].asString();
            cout << res_passId << endl;
            res_path = json_object["path"].asString();
            cout << string_img_url << endl;
            //下载图片
            JpgPuller jp;
            jp.initialize();
            if(jp.pull_image((char*)string_img_url.c_str()))
            {
                //调用识别引擎
                bool b_plate = false;
                std::string file_name;
                if(vlpr_analyze((const unsigned char *)jp.p_jpg_image, jp.jpg_size, pvpr))
                {
                    b_plate = true;
                    res_plateNo = pvpr->license;
                    res_plateColor = std::to_string(pvpr->nColor);
                    res_json_rect["left"]=pvpr->left;
                    res_json_rect["right"]=pvpr->right;
                    res_json_rect["top"]=pvpr->top;
                    res_json_rect["bottom"]=pvpr->bottom;
                    file_name = res_path;
                }
                else
                {
                    file_name = res_path + ".x.jpg";
                }
                //上传识别结果到云平台数据汇聚接口
                Json::Value json_res;
                Json::StreamWriterBuilder writerBuilder;
                std::ostringstream os;
                long res_sendtime = get_unix_ts_ms();
                json_kafka_log["logtime"]=(Json::Value::UInt64)res_sendtime;
                Json::Value json_result;
                json_result["passId"]=res_passId;
                json_result["receivetime"]=(Json::Value::UInt64)res_receivetime;
                json_result["sendtime"]=(Json::Value::UInt64)res_sendtime;
                json_result["path"]=res_path;
                json_result["engineType"]="sjk-beichuang-lpa";
                json_result["engineId"]="beichuang_01";
                json_result["plateNo"]=res_plateNo;
                json_result["plateColor"]=res_plateColor;
                if(b_plate) json_result["rect"]=res_json_rect;
                json_result["vehicleType"]="SUV";
                json_result["vehicleBrand"]="rolls-royce";
                json_result["vehicleModel"]="cullinan";
                json_result["vehicleYear"]="2035";
                json_result["vehicleColor"]="2";
                json_result["duration"]=(Json::Value::UInt64)(res_sendtime-res_receivetime);
                //kafka日志处理
                std::string log_msg;
                log_msg = res_passId;
                log_msg += "|";
                log_msg += res_plateNo;
                log_msg += "|";
                log_msg += res_plateColor;
                log_msg += "|";
                log_msg += std::to_string((int)(res_sendtime-res_receivetime));
                json_kafka_log["msg"]=log_msg;
                Json::FastWriter log_writer;
                string str_kafka_log = log_writer.write(json_kafka_log);
                g_kafka_log_queue.push(str_kafka_log);
                json_res.append(json_result);
                std::unique_ptr<Json::StreamWriter> jsonWriter(writerBuilder.newStreamWriter());
                jsonWriter->write(json_res, &os);
                string alpr_body = os.str();
                //cout << "Send:\n" << alpr_body << endl;
                JsonPusher json_pusher;
                json_pusher.initialize();
                string json_post_url = "http://172.31.49.252/data-collect/report/engine";
                string json_ret = json_pusher.push_json(json_post_url, alpr_body);
                //cout << "Josn Return:\n" << json_ret << endl;
                FileInfo file_info;
                file_info.len = jp.jpg_size;
                file_info.p = (char*)malloc(file_info.len);
                memcpy(file_info.p, (const unsigned char *)jp.p_jpg_image, file_info.len);
                file_info.filename = file_name;
                file_info.lpr_res_json = alpr_body;
                g_file_queue.push(file_info);
            }else{
                printf("Pull_jpg_error");
            }
            jp.free_memory();
        }
        pthread_mutex_unlock(&mutex_);
        auto body = "{\"code\":0}";
        res.set_content(body, "application/json");
    });
    
    thread_save_file = boost::thread(boost::bind(&task_save_file));
    
    thread_kafka_log = boost::thread(boost::bind(&task_kafka_log));
    
    cout << "The server started at port " << server_conf.server_port << "..." << endl;
    
    svr.listen(server_conf.server_ip.c_str(), server_conf.server_port);
    
    return 0;
}
