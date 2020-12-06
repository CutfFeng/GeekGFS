//
//Create by Ruifeng Xing on 2020-12-05
//
/*使用方法：
 *1.在指定目录下使用“sudo dd if=/dev/urandom of=chunkserver/chunk？/testfile？ bs=1K count=64”命令
 **生成chunk和testfile测试文件，其最终的文件目录见汇报文档
 *2.使用“sudo g++ chunkserver.cpp -o chunkserver.out”命令生成可执行文件
 *3.使用“sudo ./chunkserver.out”命令使程序进入运行状态
 *注意：master.out, client.out, chunkserver.out需要同时处于运行状态
*/
#include "iostream"
#include <stdio.h>
#include <stdlib.h>
#include <sys/msg.h>
#include <unistd.h>
#include <string.h>
#include <vector>
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <fstream>
//给定一个唯一的key
#define IPC_KEY 2;
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

size_t getcount( string filename ){//得到字符数
    size_t count = -1;
    ifstream loadfile (filename);
    if (loadfile.fail()) {      //容错处理
        std::cout << "Can not open this file" << endl;
        return count;
    }
    string d;
    while ( getline( loadfile, d ) ) //以行为单位读入文件
        count += d.size(); //累计字符数
    return count;
} 
char* readfile( string filename, int offset, int range, char buffer[] ){
    int fd;
    if( (fd = open( filename.c_str(), O_RDONLY )) < 0 ){ // 以读打开文件
        perror("Open file Failed");
        exit(1);
    }
    int ret;
    if( (ret = pread( fd, buffer, range, offset )) < 0 ){
        std::cout << "File Read Failuer!" << endl;
    }
    close( fd );
    return buffer;
}
bool writefile( string filename, int offset, int range, char buffer[] ){
    int fd;
    if( (fd = open( filename.c_str(), O_WRONLY )) < 0 ){ // 以写打开文件
        perror("Open file Failed");
        exit(1);
        return false;
    }
    size_t count = getcount( filename );
    int ret;
    if( (ret = pwrite( fd, buffer, range, offset )) < 0 ){
        std::cout << "File Read Failuer!" << endl;
        return false;
    }
    close( fd );
    return true;
}
bool writefile( string filename, int range, char buffer[] ){
    int fd;
    if( (fd = open( filename.c_str(), O_WRONLY )) < 0 ){ // 以写打开文件
        perror("Open file Failed");
        exit(1);
        return false;
    }
    size_t count = getcount( filename );
    int ret;
    if( (ret = write( fd, buffer, range )) < 0 ){
        std::cout << "File Read Failuer!" << endl;
        return false;
    }
    close( fd );
    return true;
}
void op_write( msg_queue msg, int msqid, string file_[], int offset_range[], char buffer[] ){
    msg.msgtype =666;
    int offset = offset_range[0], range = offset_range[1];
    size_t count = 65276-getcount(file_[0]);
    cout << file_[0] << " " << file_[1] << file_[2] << endl;
    if ( (range > count) && (offset == 0) ){//append第一次，数据块不够长
        bool pri_file = writefile( file_[0], count, buffer );
        if ( pri_file == false ){
            strcpy( msg.msgtext, "Write False.Please Try Again!" );
            msgsnd( msqid, &msg, sizeof(msg.msgtext), 0 );
        }
        else{
            //写secondary chunk
            bool sec_file1 = writefile( file_[1], range, buffer );
            bool sec_file2 = writefile( file_[2], range, buffer );
            if ( (sec_file1 == true) && ( sec_file2 == true )) {
                strcpy( msg.msgtext, "Write Successfully!");
                msgsnd( msqid, &msg, sizeof(msg.msgtext), 0 );
                msg.msgtype = 555;
                strcpy( msg.msgtext, "Over." );
                msgsnd( msqid, &msg, sizeof(msg.msgtext), 0 );//结束client里write的for循环
                strcpy( msg.msgtext, "FileBlock is not enough.Please try again on other block!" );
                msgsnd( msqid, &msg, sizeof(msg.msgtext), 0 );
            }
            else{
                strcpy( msg.msgtext, "Write False.Please Try Again!" );
                msgsnd( msqid, &msg, sizeof(msg.msgtext), 0 );
            } 
        }
    }
    else{
        std::cout << "I'm the second write." << endl;
        bool pri_file = writefile( file_[0], offset, range, buffer );
        if ( pri_file == false ){
            strcpy( msg.msgtext, "Write False.Please Try Again!" );
            msgsnd( msqid, &msg, sizeof(msg.msgtext), 0 );
        }
        else{
            //写secondary chunk
            bool sec_file1 = writefile( file_[1], offset, range, buffer );
            bool sec_file2 = writefile( file_[2], offset, range, buffer );
            if ( (sec_file1 == true) && ( sec_file2 == true )) {
                strcpy( msg.msgtext, "Write Successfully!");
                msgsnd( msqid, &msg, sizeof(msg.msgtext), 0 );
            }
            else{
                strcpy( msg.msgtext, "Write False.Please Try Again!" );
                msgsnd( msqid, &msg, sizeof(msg.msgtext), 0 );
            }   
        }
    }
    msg.msgtype = 666;
    strcpy( msg.msgtext, "Over." );
    msgsnd(msqid, &msg, sizeof(msg.msgtext), 0);//添加消息
    msg.msgtype = 555;
    strcpy( msg.msgtext, "Over." );
    msgsnd(msqid, &msg, sizeof(msg.msgtext), 0);//添加消息
}
void op_read( msg_queue msg, int msqid ){  
    string filename = msg.msgtext;
    int offset = msg.msgint[1];
    int range = msg.msgint[2];
    char buffer[4096];
    msg.msgtype = 777;
    do{
        readfile( filename, offset, range, buffer );
        strcpy( msg.msgtext, buffer );
        msgsnd(msqid, &msg, sizeof(msg.msgtext), 0);//添加消息
        offset += 4096;
        range -= 4096; 
    }while( range >= 0 );
    strcpy( msg.msgtext, "Over." );
    msgsnd(msqid, &msg, sizeof(msg.msgtext), 0);//添加消息
    msg.msgtype = 555;
    strcpy( msg.msgtext, "Over." );
    msgsnd(msqid, &msg, sizeof(msg.msgtext), 0);//添加消息
    return;
}
string* subfile_( msg_queue msg, int msqid, string file_[] ){//分离主次副本的路径
    string str = msg.msgtext;
    int pos = 0;
    for ( int i=0; i<3; i++ ){
        int strpos = str.find( " ", pos );
        file_[i] = str.substr( 0, strpos );
        str = str.substr( strpos+1 );
    }
    strcpy( msg.msgtext, "Please input the data you want to update:");
    msg.msgtype = 666;
    msgsnd( msqid, &msg, sizeof(msg.msgtext), 0 );
    strcpy( msg.msgtext, "Over." );
    msgsnd( msqid, &msg, sizeof(msg.msgtext), 0 );
    msg.msgtype = 555;
    strcpy( msg.msgtext, "Over." );
    msgsnd(msqid, &msg, sizeof(msg.msgtext), 0);//添加消息
    return file_;
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
    string file_[3];
    int offset_range[2];
    for(;;){//读消息
        msgrcv(msqid, &msg, 4096, -334, 0);// 读类型为rcvtype的第一个消息
        if ( msg.msgtype == 333 ){
            op_read( msg, msqid );
        }
        else if( msg.msgtype == 222 ){//222传偏移量，221传data
            offset_range[0] = msg.msgint[1];
            offset_range[1] = msg.msgint[2];
            subfile_( msg, msqid, file_ );
        }
        else if( msg.msgtype == 221 ){
            op_write( msg, msqid, file_, offset_range, msg.msgtext );
        }
        std::cout << "Changemsg Done." << endl;
    }
}

int main()
{
    changemsg();
    return 0;
}