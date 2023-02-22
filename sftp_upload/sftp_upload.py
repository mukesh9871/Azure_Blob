import os, sys
from datetime import datetime
import socket
import ssh2.session
import ssh2.sftp
from ssh2.session import Session
from ssh2.sftp import LIBSSH2_FXF_READ, LIBSSH2_SFTP_S_IRUSR, LIBSSH2_SFTP_ST_NOSUID, LIBSSH2_SFTP_S_IRWXO, LIBSSH2_SFTP_S_IRWXG, LIBSSH2_SFTP_S_IRWXU, LIBSSH2_SFTP_S_IFREG
from ssh2.sftp import LIBSSH2_FXF_CREAT, LIBSSH2_FXF_WRITE

# Set up the connection parameters
hostname = 'clouddrive.ipetronik.com'
port = 22
username = 'IPE_Loggers'
password = 'IPE_Loggers'

# Set up the file upload parameters
local_path = '/mnt/d/ipeauthmukesh.txt'
remote_path = '/mukesh'

# Create socket connection
def buildSocket():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        print(f"host = {hostname}")
        sock.connect((hostname, port))
        sock.settimeout(None)
        print(f"Socket created")
        return sock
    except:
        print(f"Exception in socket: {sys.exc_info()[1]} {sys.exc_info()[0]}")

    # Build session
def buildSession(sock):
    try:
        session = Session()
        print(f"Session object created  {datetime.now()}")
        session.set_timeout(10000)
        print(f"Session created -2      {datetime.now()}")
        session.handshake(sock)
        print(f"handshake done          {datetime.now()}")
        session.userauth_password(username, password)
        print(f"userauth_password done  {datetime.now()}")
        session.set_timeout(0)
        print(f"Session created         {datetime.now()}")
        return session
    except:
        print(f"Exception in session connect: {sys.exc_info()[1]} {sys.exc_info()[0]}")

lsock = buildSocket()
lsession = buildSession(lsock)
# session init
sftp = lsession.sftp_init()
print("sftp inti done.")
#dest_path = hostname + remote_path

"""# Open an SSH session and authenticate
print(f"Before sock obj create  {datetime.now()}")
lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print(f"socket object created   {datetime.now()}")
lsock.connect((hostname, port))
print(f"socket connected       {datetime.now()}")
lsession = ssh2.session.Session()
print(f"session object created {datetime.now()}")
lsession.handshake(lsock)
print(f"session handshak done  {datetime.now()}")
lsession.userauth_password(username, password)
print(f"userauth_password done {datetime.now()}")"""

f_flags = LIBSSH2_FXF_CREAT | LIBSSH2_FXF_WRITE
mode = LIBSSH2_SFTP_ST_NOSUID | LIBSSH2_SFTP_S_IRWXO | LIBSSH2_SFTP_S_IRWXG | LIBSSH2_SFTP_S_IRWXU
remote_file = sftp.open(remote_path, f_flags, mode)

# Upload the file in chunks and display the progress
total_size = os.path.getsize(local_path)
chunk_size = 1024 * 1024  # 1 MB
bytes_sent = 0
with open(local_path, 'rb') as local_file:
    while True:
        chunk = local_file.read(chunk_size)
        if not chunk:
            break
        remote_file.write(chunk)
        bytes_sent += len(chunk)
        percent_complete = int((bytes_sent / total_size) * 100)
        print(f"Upload progress: {percent_complete}%")

# Close the file and SFTP session
remote_file.close()
#sftp.close()

# Close the SSH session
lsession.disconnect()
lsock.close()