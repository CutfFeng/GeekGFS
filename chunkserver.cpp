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

char* read( string filename, int offset, int range, char buffer[] ){
    cout << "chunkserver's filename: " << filename << endl;
    // char buffer[4096];
    int fd;
    if( (fd = open( filename.c_str(), O_RDONLY )) < 0 ){ // 以读打开文件
        perror("Open file Failed");
        exit(1);
    }
    // cout << "fd: " << fd << " range: " << range << " offset: " << offset << endl;
    int ret;
    if( (ret = pread( fd, buffer, range, offset )) < 0 ){
        cout << "File Read Failuer!" << endl;
    }
    // cout << "ret: " << ret << endl;
    close( fd );
    return buffer;
}

int creatmsq(){
    int msqid;
    key_t key;
    
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
}

void changemsg( long msgtype, int* msgint, long rcvtype, int msqid ){
    // cout << "I'm changemsg." << endl;
    msg_queue msg;
    for(;;){//读消息
        msgrcv(msqid, &msg, 4096, rcvtype, 0);// 读类型为rcvtype的第一个消息
        string filename = msg.msgtext;
        int offset = msg.msgint[1];
        int range = msg.msgint[2];
        char buffer[4096];
        // cout << "filename,offset,range:" << filename << offset << range << endl;
        do{
            read( filename, offset, range, buffer );
            // cout << "changensg's buffer: " << buffer << endl;
            strcpy( msg.msgtext, buffer );
            cout << "msgtext: " << msg.msgtext << endl;
            msg.msgtype = 666;
            msgsnd(msqid, &msg, sizeof(msg.msgtext), 0);//添加消息
            // cout << "ret: " << ret << endl;
            offset += 4096;
            range -= 4096; 
        }while( range >= 0 );
        // cout << " changemsg Done." << endl;
    }
}

int main()
{
    int msqid = creatmsq();
    changemsg( 666, NULL, 777, msqid );
    return 0;
}