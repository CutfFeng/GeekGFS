#include "iostream"
#include <stdio.h>
#include <stdlib.h>
#include <sys/msg.h>
#include <unistd.h>
#include <string.h>
#include <vector>
#include <sys/fcntl.h>
#include <sys/stat.h>
// 用于创建一个唯一的key
#define MSG_FILE "/home/xrfpc/Documents/distributed-finalwork1/message"
using namespace std;

class msg_queue
{
public:
    long msgtype;   //消息类型
    int index;  //chunk的index
    int msgint[3];     //消息中的整数类型，如chunkIndex, chunkHandle
    char msgtext[4096];//消息中的char类型,如filename, chunkLocation
    msg_queue();
    ~msg_queue();
};
msg_queue::msg_queue(){

}
msg_queue::~msg_queue(){

}

char* read( string filename, int offset, int range ){
    cout << "chunkserver's filename: " << filename << endl;
    char buffer[4096];
    int fd;
    if( (fd = open( filename.c_str(), O_RDONLY )) < 0 ){ // 以读打开文件
        perror("Open file Failed");
        exit(1);
    }
    cout << "fd: " << fd << " range: " << range << " offset: " << offset << endl;
    int ret;
    if( (ret = read( fd, buffer, range )) < 0 ){
        cout << "File Read Failuer!" << endl;
    }
    cout << "ret: " << ret << endl;
    close( fd );
    return buffer;
}

void changemsg( long msgtype, int* msgint, long rcvtype ){
    // cout << "I'm changemsg." << endl;
    int msqid;
    key_t key;
    msg_queue msg;
    
    // 获取key值
    if((key = ftok(MSG_FILE,'a')) < 0)
    {
        perror("ftok error");
        exit(1);
    }
 
    // 创建消息队列
    if ((msqid = msgget(key, IPC_CREAT|0777)) == -1)
    {
        perror("msgget error");
        exit(1);
    }
    for(;;){//读消息
        msgrcv(msqid, &msg, 4096, rcvtype, 0);// 读类型为rcvtype的第一个消息
        msg.msgtype = 666;
        string filename = msg.msgtext;
        int offset = msg.msgint[1];
        int range = msg.msgint[2];
        do{
            strcpy( msg.msgtext, read( filename, offset, range ) );
            msgsnd(msqid, &msg, sizeof(msg.msgtext), 0);//添加消息
            offset += 4096;
            range -= 4096; 
        }while( range >= 4096 );
    }
}

int main()
{
    changemsg( 666, NULL, 777 );
    return 0;
}