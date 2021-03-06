# The filter calls and external process when a new object of the file is uploaded. 
# The process will be called for each 10MB uploaded so the process should wait a reasonable time to allow s3ql to upload it.
# paramters : redis host, redis port, extension key, process key
import redis
import subprocess

def init (param):
    return None

def readxform_c (self,buf,param):
    arr = bytearray(buf)
    buf = bytes(arr)
    return buf

def writexform_c (self,buf,param):
    arr = bytearray(buf)
    buf = bytes(arr)
    return buf

def get_path(id_, conn):

    name = conn.get_row("SELECT contents_v.name FROM contents_v,blocks,inode_blocks where contents_v.inode=inode_blocks.inode and blocks.id=inode_blocks.block_id and blocks.obj_id=?", (id_,))
    return name

# HOST PORT KEY KEYCOMMAND
# Gets from redis list of extensions (txt, c3d)..
def get_redis(self, param):
    params = param.split()
    redis_host = str(params[0]).lower()
    redis_port =  int(params[1])
    r = redis.Redis(connection_pool=redis.ConnectionPool(host=redis_host, port=redis_port, db=0))
    files = r.get(params[2])
    command = r.get(params[3])
    return (files,command)

def flush(self,param):
    buf = None
    try:
       self.db
       self.fh.fh.key
    except:
       return buf
    try:
       id_ = get_path(int(self.fh.fh.key.split("_")[2]), self.db)
    except:
       return buf
    (files,command) = get_redis(self,param)
    files = files.decode("utf-8")
    command = command.decode("utf-8")
    args = command.split()
    fname = id_[0].decode("utf-8")
    splitfname = fname.split(".")
    extension = splitfname[len(splitfname)-1]
    if extension in files.split():
       print ("Found",splitfname)
       args = args+[fname]
       subprocess.call(args)
   # except:
    #   return buf
    return buf
