#include "iostream"
#include <stdio.h>
#include <stdlib.h>
#include <sys/msg.h>
#include <unistd.h>
#include <string.h>
#include <vector>
#include <sys/stat.h>
#include <sys/fcntl.h>
// 用于创建一个唯一的key
// #define MSG_FILE "/home/xrfpc/Documents/distributed-finalwork1/message"
#define IPC_KEY1 1
#define IPC_KEY2 2
using namespace std;

class msg_queue
{
public:
    long msgtype;   //消息类型
    int index;  //chunk的index
    int msgint[3];     //消息中的整数类型，如chunkIndex, chunkHandle
    char msgtext[4096];//消息中的char类型,如filename, chunkLocation
    msg_queue();
    msg_queue( long msgtype, int chunkIndex, char filename[4096] );
    msg_queue( long msgtype, int* msgint, string file_fd );
    ~msg_queue();
};
msg_queue::msg_queue(){

}
msg_queue::msg_queue( long msgtype, int chunkIndex, char filename[4096] ){
    this->msgtype = msgtype;
    this->index = chunkIndex;
    for(int i=0; i<3; i++){
        this->msgint[i] = -1;
    }
    // cout << "111" << endl;
    strcpy( this->msgtext, filename );
    // cout << "msg's filename is " << msgtext << endl;
}
msg_queue::msg_queue( long msgtype, int* mint, string file_fd ){
    this->msgtype = msgtype;
    strcpy( msgtext, file_fd.c_str() );
    for ( int i=0; i<3; i++ ){
        this->msgint[i] = mint[i];
    }
}
msg_queue::~msg_queue(){

}

msg_queue changemsg( long msgtype, int chunkIndex, char filename[4096], long rcvtype, int msqid ){//和master交换消息
    cout << "changemsg1's filename is " << filename << endl;
    msg_queue msg( msgtype, chunkIndex, filename );
    // cout << "msqid is " << msqid << endl;
    msgsnd(msqid, &msg, sizeof(msg.msgtext), 0);//添加消息
    for(;;){//读消息
        msgrcv(msqid, &msg, 4096, rcvtype, 0);// 返回类型为rcvtype的第一个消息
        return msg;
    }
}
void changemsg( long msgtype, string file_fd, int* msgint, long rcvtype, int msqid ){//和chunkserver交换消息
    // cout << "I'm changemsg." << endl;
    msg_queue msg( msgtype, msgint, file_fd );
    // cout << "fileLocation is " << msg.msgtext << endl;
    // cout << "msqid is " << msqid << msgint[1] << msgint[2] << endl;
    msgsnd(msqid, &msg, sizeof(msg.msgtext), 0);//添加消息
    for(;;){//读消息
        cout << "Message From Chunkserver:";
        msgrcv(msqid, &msg, 4096, rcvtype, 0);// 返回类型为rcvtype的第一个消息
        cout << msg.msgtext << endl;
        if( strcmp( msg.msgtext, "Over." ) == 0 )
            break;
    }
    // cout << "555" << endl;
    for(;;){//读消息
        // cout << "Message From Chunkserver:";
        msgrcv(msqid, &msg, 4096, 555, 0);// 返回类型为rcvtype的第一个消息
        // cout << msg.msgtext << endl;
        if( strcmp( msg.msgtext, "Over." ) == 0 )
            break;
    }
    return;
}

void readfile( string file_fd, int handle, size_t offset, size_t range, int msqid ){
    // cout << "I'm read." << endl;
    int msgint[3] = {handle, offset, range};
    changemsg( 333, file_fd, msgint, 777, msqid );
    return;
}
void writefile( string file_fd, int* handle, size_t offset, size_t range, int msqid ){
    // cout << "I'm write." << endl;
    int msgint[3] = {-1, offset, range};
    changemsg( 222, file_fd, msgint, 666, msqid );
    // cout << "input data:" << endl;
    string data = "";
    cin >> data;
    // cout << "data is " << data << endl;
    changemsg( 221, data, msgint, 666, msqid );
    cout << "writefile done." << endl;
}
void append( char filename[], int chunkIndex, string file_fd, int* handle, size_t offset, size_t range, int msqid1, int msqid2 ){
    writefile( file_fd,  handle,  offset, range, msqid2 );
    msg_queue msg;
    for (;;){
        cout << "I'm here." << endl;
        msgrcv( msqid2, &msg, 4096, 555, 0 );
        cout << msg.msgtext << endl;
        if( strcmp( msg.msgtext, "Over." ) == 0 )
            return;
        else{
            // cout << "I'm here and chunkIndex is " << chunkIndex << endl;
            msg_queue tempmsg = changemsg( 222, chunkIndex, filename, 999, msqid1 );
            string file_fd = tempmsg.msgtext;
            string str = filename;
            string name = "/" + str;
            int pos = 0,addpos;
            for( int i=0; i<3; i++ ){
                addpos = file_fd.find( " ", pos );
                // cout << "addpos is " << addpos << endl;
                file_fd.insert( addpos, name );
                pos = addpos+name.size()+1;
            }
            // cout << "fd is " << file_fd << endl;
            writefile( file_fd, handle, offset, range, msqid2 );
            // msgrcv( msqid2, &msg, 4096, 555, 0 );
            // cout << msg.msgtext << endl;
            // if( strcmp( msg.msgtext, "Over." ) == 0 )
            //     return;
        }
    }
}
void Delete(){
    
}

int creatmsq( key_t IPC_KEY ){     //创建消息队列,返回队列id
    int msqid;
    key_t key;
    key = IPC_KEY;
 
    // 打开消息队列
    if ((msqid = msgget(key, IPC_CREAT)) == -1)
    {
        perror("msgget error");
        exit(1);
    }
    return msqid;
}

class client
{
private:
    int command;
    char filename[256];
    size_t offset;     //起始位置
    size_t byterange;
    int chunkIndex;
    int chunkHandle[3];
    char chunkLocation[256];
    int msqid1;//与master通信
    int msqid2;//与chunkserver通信
public:
    client( int comd, string name, int offset, int range, int msqid1, int msqid2 );
    ~client();
    void operation ();
};
client::client( int comd, string name, int offset, int range, int msqid1, int msqid2 ){
    // cout << "I'm here." << endl;
    this->command = comd;
    strcpy( this->filename, name.c_str() );
    // cout << "filename: " << filename << endl;
    this->offset = offset;
    this->byterange = range;
    this->chunkIndex = (offset/(64*1024) + 1);   //计算得到chunkIndex
    // cout << "chunkIndex is " << chunkIndex << endl;
    long msgtype = 333,rcvtype = 999;
    this->msqid1 = msqid1;
    this->msqid2 = msqid2;
    msg_queue msg = changemsg( msgtype, chunkIndex, filename, rcvtype, msqid1 );
    // cout << "I'm client.Msgtext is " << msg.msgtext << endl;
    for (int i=0; i<3; i++){
        this->chunkHandle[i] = msg.msgint[i];
    }
    strcpy( this->chunkLocation, msg.msgtext );
}
client::~client(){
    
}
void client::operation(){
    // cout << "command is " << command << endl;
    if( command == 0 ){// read的实现
        string file_fd = "";
        for( int i=0; this->chunkLocation[i]!=' '; i++ ){
            file_fd = file_fd + chunkLocation[i];
        }
        file_fd = file_fd + "/" + this->filename;
        // cout << "file_fd is " << file_fd << endl;
        readfile( file_fd, chunkHandle[0], offset, byterange, msqid2 );
    }
    else if( command == 1 ){//write的实现
        string file_fd = chunkLocation;
        string str = this->filename;
        string name = "/" + str;
        int pos = 0,addpos;
        for( int i=0; i<3; i++ ){
            addpos = file_fd.find( " ", pos );
            // cout << "addpos is " << addpos << endl;
            file_fd.insert( addpos, name );
            pos = addpos+name.size()+1;
        }
        // cout << "file_fd is " << file_fd << endl;
        writefile( file_fd, chunkHandle, offset, byterange, msqid2 );
    }
    else if ( command == 2 ){//append的实现
        string file_fd = chunkLocation;
        string str = this->filename;
        string name = "/" + str;
        int pos = 0,addpos;
        for( int i=0; i<3; i++ ){
            addpos = file_fd.find( " ", pos );
            // cout << "addpos is " << addpos << endl;
            file_fd.insert( addpos, name );
            pos = addpos+name.size()+1;
        }
        append( filename, chunkIndex, file_fd, chunkHandle, offset, byterange, msqid1, msqid2 );
    }
    else if ( command == 3)
        Delete();
    else{
        cout << "There is no this option!" << endl;
    }
    return;
}

void print( int msqid1, int msqid2 ){
    // cout << "a" << endl;
    while (1){
        string name = "";
        int offset=0,range;
        int comd;
        cout << "Please chose your option:" << endl;
        cout << "0:read; 1:write; 2:append; 3:exit; 4:delete" << endl;
        cin >> comd;
        if( comd == 3 ){
            cout << "Bye-bye!" << endl;
            break;
        }
        else{
            cout << "filename:";
            cin >> name ;
            if( (comd == 0) || (comd == 1)){
                cout << "begin:";
                cin >> offset ;
            }
            cout << "range:";
            cin >> range;
            client client( comd, name, offset, range, msqid1, msqid2 );
            client.operation();
        }
    }
}

int main()
{
    int msqid1 = creatmsq(IPC_KEY1);//获得与master通信的消息队列id
    int msqid2 = creatmsq(IPC_KEY2);//获得与chunkserver通信的消息队列id
    print( msqid1, msqid2 );
    return 0;
}