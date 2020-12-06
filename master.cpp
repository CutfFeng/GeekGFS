//
//Create by Ruifeng Xing on 2020-12-05
//
/*使用方法：
 *1.需要先生成chunk和testfile测试文件，生成方法位于chunkserver.cpp的注释中
 *2.使用“sudo g++ master.cpp -o master.out”命令生成可执行文件
 *3.使用“sudo ./master.out”命令使程序进入运行状态
 *注意：master.out, client.out, chunkserver.out需要同时处于运行状态
*/
#include "iostream"
#include "vector"
#include <stdio.h>
#include <stdlib.h>
#include <sys/msg.h>
#include <unistd.h>
#include <string.h>
#include <sys/fcntl.h>
#include <map>
#include <ctime>
#include <fstream>
// 给定一个唯一的key
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
    map<string,int> file_tag;
    master();
    ~master();
    string get_location( string name, int index );
    int* get_handle( string name, int index ); 
    void Delete( string name );
    void writefile();
};
master::master(){
    // cout << "I'm master." << endl;
    filename.push_back( "testfile1" );
    filename.push_back( "testfile2" );
    file_tag.insert(pair<string,int>("testfile1",1));
    file_tag.insert(pair<string,int>("testfile2",1));
    string ss = "../chunkserver/chunk";
    for( int i=0; i<6; i++ ){//创建6个chunkserver的路径
        ss = ss + std::to_string(i);
        chunkLocation.push_back( ss );
        ss = "../chunkserver/chunk";
    }
    int a[6] = {1,2,3,0,4,5},b[6] = {3,4,5,0,1,2};
    vector<int> c;
    for( int i=0; i<6; i++ ){
        c.push_back( a[i] );
    }
    fileId_chunkId.push_back(c);
    vector<int> d;
    for( int i=0; i<6; i++ ){
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
    if ( (i<0) || (i>5) )
        return NULL;
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
void master::Delete( string name ){
    map<string,int>::iterator iter =  file_tag.find(name);
    if(iter != file_tag.end()){
        iter->second = 0;
    }
    else
        cout<<"Do not find this file!"<<endl;
}
void master::writefile(){
    std::ofstream writefile;
    //打开文件
    writefile.open("../masterdata.txt");
    writefile << "filename:\n";
    for(int i = 0; i < filename.size(); i++)
    {
        //写入数据
        writefile << filename[i];
    }
    //关闭文件
    writefile.close();
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

int creatmsq(){
    int msqid;
    key_t key;
    key = IPC_KEY;//给定唯一的key值

    // 创建消息队列
    if ((msqid = msgget(key, IPC_CREAT|0777)) == -1)
    {
        perror("msgget error");
        exit(1);
    }
    return msqid;
}

void changemsg(){
    int msqid = creatmsq();
    msg_queue msg;
    //获取系统时间
    time_t now = time(0);
    tm *ltm = localtime(&now);
    int hour = ltm->tm_hour;
    
    for(;;){  
        master mst;
        time_t now = time(0);
        tm *templtm = localtime(&now);
        int temphour = templtm->tm_mday; 
        if( temphour == (hour+7) ){//每七小时更新一次
            // mst.writefile();
        }  
           
        msgrcv(msqid, &msg, 4096, -333, 0);// 读从客户端发送过来的消息
        string name = msg.msgtext;
        if( msg.msgtype == 221 ){//client请求删除文件
            mst.Delete(name);
        }
        else{
            map<string,int>::iterator iter =  mst.file_tag.find(name);
            //被访问则修改标志位
            if( (iter != mst.file_tag.end()) && (iter->second = 0) ){
                iter->second = 1;
            }
            // cout << "msg.index is " << msg.index << endl;
            int index;
            if( msg.msgtype == 222 ){
                //append函数请求开辟新的chunk
                index = msg.index + 1;
            }
            else{
                index = msg.index;
            }

            msg.msgtype = 999; // 添加消息，类型为999
            strcpy( msg.msgtext, mst.get_location( name, index ).c_str() );
            int *handle = mst.get_handle( name, index );
            for (int i=0; i<3; i++){
                msg.msgint[i] = *(handle+i);
            }
            msgsnd(msqid, &msg, sizeof(msg.msgtext), 0);
        }
        cout << "Change messge done." << endl;
    }     
}

int main()
{
    changemsg( );
    return 0;
}