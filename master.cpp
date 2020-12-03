#include "iostream"
#include "vector"
#include <stdio.h>
#include <stdlib.h>
#include <sys/msg.h>
#include <unistd.h>
#include <string.h>
#include <sys/fcntl.h>
// 用于创建一个唯一的key
// #define MSG_FILE "/home/xrfpc/Documents/distributed-finalwork1/message"
#define IPC_KEY 1;
using namespace std;

class master
{
private:
    vector<string> filename;
    vector<string> chunkLocation;
    vector<vector<int>> fileId_chunkId;    //(index-1)*3是第一个下标，继续往后遍历两个
    vector<int> chunkhandle;
public:
    master();
    ~master();
    string get_location( string name, int index );
    int* get_handle( string name, int index ); 
};
master::master(){
    // cout << "I'm master." << endl;
    filename.push_back( "testfile1" );
    filename.push_back( "testfile2" );
    string ss = "../chunkserver/chunk";
    for( int i=0; i<6; i++ ){//创建6个chunkserver的路径
        ss = ss + std::to_string(i);
        chunkLocation.push_back( ss );
        ss = "../chunkserver/chunk";
    }
    int a[6] = {1,2,3,0,4,5},b[3] = {3,4,5};
    vector<int> c;
    for( int i=0; i<6; i++ ){
        c.push_back( a[i] );
    }
    fileId_chunkId.push_back(c);
    vector<int> d;
    for( int i=0; i<3; i++ ){
        d.push_back( b[i] );
    }
    fileId_chunkId.push_back( d );
}
master::~master(){

}
string master::get_location( string name, int index ){
    // cout << "I'm get_location." << endl;
    int size = filename.size();
    int fileId = -1;
    for ( int i=0; i<size; i++ ){
        if ( filename[i].compare( name ) == 0 ){
            fileId = i;
            break;
        }    
    }
    if ( fileId == -1 ){
        cout << "没有该文件名！" << endl;
        return NULL;
    }
    string location = "";   //存放chunk位置
    int chunkId[3];         //根据chunkId去chunkLocation数组里找它的位置
    int i = (index-1)*3;    //chunk在二维数组fileId_chunkId的第一个下标
    for( int j=0; j<3; j++ ){
        chunkId[j] = fileId_chunkId[fileId][i];
        i++;
        location = location + chunkLocation[chunkId[j]] + " ";
    }
    // cout << "location is " << location << endl;
    return location;
}
int* master::get_handle( string name, int index ){
    int size = filename.size();
    int fileId = -1;
    for ( int i=0; i<size; i++ ){
        if ( filename[i].compare( name ) == 0 ){
            fileId = i;
            break;
        }    
    }
    if ( fileId == -1 ){
        cout << "没有该文件名！" << endl;
        return NULL;
    }
    int *handle = new int[3];
    string location = "";   //存放chunk位置
    int chunkId[3];         //根据chunkId去chunkLocation数组里找它的位置
    int i = (index-1)*3;    //chunk在二维数组fileId_chunkId的第一个下标
    for( int j=0; j<3; j++ ){
        chunkId[j] = fileId_chunkId[fileId][i];
        i++;
        location = location + chunkLocation[chunkId[j]] + "/" + name;
        int fd = open( location.c_str(),O_RDWR );//获得文件描述符
        handle[j] = fd ;
        close (fd);
        location = "";
    }
    return handle;
}

class msg_queue
{
public:
    long msgtype;   //消息类型
    int index;
    int msgint[3];     //消息中的整数类型，如chunkIndex, chunkHandle
    char msgtext[4096];//消息中的char类型,如filename, chunkLocation
    msg_queue();
    ~msg_queue();
};
msg_queue::msg_queue(){
    for ( int i=0; i<3; i++ ){
        msgint[i] = -1;
    }
}
msg_queue::~msg_queue(){

}

void changemsg(){
    int msqid;
    key_t key;
    key = IPC_KEY;
    msg_queue msg;

    // 获取key值
    // if ((key = ftok(MSG_FILE, 'b')) < 0) 
    // {
    //     perror("ftok error");
    //     exit(1);
    // }
 
    // 打开消息队列
    if ((msqid = msgget(key, IPC_CREAT)) == -1) 
    {
        perror("msgget error");
        exit(1);
    }
    
    for(;;){        
        msgrcv(msqid, &msg, 4096, 999, 0);// 读从客户端发送过来的消息
        // cout << "222" << endl;
        string name = msg.msgtext;
        int index = msg.index;
        // cout << "filename is " << msg.msgtext << endl;
        // cout << "chunkIndex is " << index << endl;

        msg.msgtype = 888; // 添加消息，类型为888
        master mst;
        strcpy( msg.msgtext, mst.get_location( name, index ).c_str() );
        // cout << "111" << endl;
        int *handle = mst.get_handle( name, index );
        // cout << "*handle is " << handle[0] << endl;
        for (int i=0; i<3; i++){
            msg.msgint[i] = *(handle+i);
            // cout << "333" << endl;
        }
        // cout << "I'm here." << endl;
        msgsnd(msqid, &msg, sizeof(msg.msgtext), 0);
        cout << "Change messge done." << endl;
    }
}

int main()
{
    changemsg();
    return 0;
}