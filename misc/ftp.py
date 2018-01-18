#-*-coding:utf-8-*-  
  
  
import os  
import re
import time  
from ftplib import FTP  



#############################
FTP_SERVER='10.1.xx.xx'
FTP_PORT=22
INTERVAL=20  #in seconds
FTP_DEBUG_LEVEL=0
FTP_MAX_TRIES=10



#############################

USER='user'  
PWD ='pass'  
BASE_PATH='/base' 


file_reg=r'Hello_.{3}_XX_(a|b|c)\.bz2'


li     = []
li_pre = []

print '\r\n'
print 'Now Date: ' + time.strftime('%Y%m%d',time.localtime(time.time()))  + '\r\n'

def print_ts(message):
     print "[%s] %s"%(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), message)

def run(interval) :
     print_ts("-"*79)
     #print_ts("Command %s"%command)
     print_ts("Starting every %s seconds."%interval)
     print_ts("-"*79)
     while True:
         try:
             # sleep for the remaining seconds of interval
             time_remaining = interval-time.time()%interval
             print_ts("Wait until %s (%s seconds)..."%((time.ctime(time.time()+time_remaining)), time_remaining))
             time.sleep(time_remaining)

             # execute the command
             downloadfile()
             print_ts("-"*79)
             #print_ts("Command status = %s."%status)
         except Exception, e:
             print e

def mkdir(path):

    path=path.strip()

    # path=path.rstrip("\\")

    # Exist     True
    # None   False
    isExists=os.path.exists(path)

    if not isExists:
        os.makedirs(path)  
        print_ts("Create dir %s" % path)
        return True
    else:
        return False
  
def ftpconnect():  
    maxTryNum=FTP_MAX_TRIES
    ftp=FTP()
    ftp.set_pasv(False)
    ftp.set_debuglevel(FTP_DEBUG_LEVEL)
    for tries in range(maxTryNum):  
        try:  
            ftp.connect(FTP_SERVER,FTP_PORT)  
            ftp.login(USER,PWD)  
        except:  
            if tries <(maxTryNum-1):  
                print_ts("Not connected, retry, left:%d times." % tries)
                continue
            else:  
                print_ts("Has tried %d times, all failed!" % maxTryNum)
                break 
    return ftp  


def downloadfile():
    
    DATE= time.strftime('%Y%m%d',time.localtime(time.time()))  
    LOCAL_DOWNLOAD_PATH='D:/tmp/' + DATE + '/'        # temp dir
    FTP_PATH=BASE_PATH + DATE + '/'

    print_ts("Connet to FTP...")
    ftp = ftpconnect()      
    print_ts("FTP Resp:%s" % ftp.getwelcome() )

    global li
    global li_pre

    li_pre = li
    li     = ftp.nlst(FTP_PATH)
    new_li = list(set(li_pre)^set(li))

    mkdir(LOCAL_DOWNLOAD_PATH)
    print_ts("Checking files: %d..." % len(new_li) )

    for remotefile in new_li:  
        #print os.path.basename( remotefile )
        basename = os.path.basename( remotefile )
        f_match = re.search(file_reg, basename)
        l_size = 0L
        f_size = ftp.size(remotefile)
        
        # print_ts("Remote file:%s, size:%d." % (basename, f_size))
        if f_match:
            localfile = LOCAL_DOWNLOAD_PATH + basename  
            # Check the file exist or not, then download
            # Check local file isn't existes and get the local file size
            
            if os.path.exists(localfile):
                # print_ts("-"*79)
                # print_ts("File %s already exist, checking size..." % basename)
                l_size  = os.stat(localfile).st_size
                
            if l_size >= f_size:     
                # print_ts("Local file %s is ok, no need to download, skip..." % basename)
                continue                
            print_ts("-"*79)
            print_ts("Downloading %s..." % localfile)

            blocksize = 1024 * 1024
            ftp.voidcmd('TYPE I')

            conn = ftp.transfercmd('RETR ' + remotefile, l_size)
            lwrite=open(localfile,'ab')
            while True:
                data=conn.recv(blocksize)
                if not data:
                   break 
                lwrite.write(data)

            lwrite.close()
            ftp.voidcmd('NOOP')
            ftp.voidresp()
            conn.close()

            
    # end for loop

    print_ts("Download complete for now, will check new files later.")
    print_ts("#"*79)
    ftp.quit()
  
if __name__=="__main__":  
    run(INTERVAL)

