# The filter adds additional metadata to files ( objects ) . 
# paramters : redis host, redis port, (substring - metadata)*
import redis
import subprocess

def init (param):
    return None

def readxform_c (self,buf,param):
    arr = bytearray(buf)
    buf = bytes(arr)
    if ("Tag1" in self.metadata and self.metadata["Tag1"] == 'SECRET'):
      return bytes(len(arr))
    return buf

def writexform_c (self,buf,param):
    arr = bytearray(buf)
    buf = bytes(arr)
    return buf

def get_path(id_, conn):

    name = conn.get_row("SELECT contents_v.name FROM contents_v,blocks,inode_blocks where contents_v.inode=inode_blocks.inode and blocks.id=inode_blocks.block_id and blocks.obj_id=?", (id_,))
    return name

# HOST PORT KEY KEYCOMMAND
# Gets from redis pairs of substring#metadata tag.
def get_redis(self, param):
    params = param.split()
    redis_host = str(params[0]).lower()
    redis_port =  int(params[1])
    r = redis.Redis(connection_pool=redis.ConnectionPool(host=redis_host, port=redis_port, db=0))
    files = r.get(params[2])
    return files

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
    
    files = get_redis(self,param)
    files = files.decode("utf-8")
    fname = id_[0].decode("utf-8")
    files = files.split()
    ntag = 1
    for i in files:
        (nfile, tag) = i.split("#")
        print (nfile, fname)
        if nfile in fname:
           print ("Found", fname)
           self.fh.fh.headers['X-Object-meta-TAG'+str(ntag)] = tag
           ntag = ntag + 1
    return buf
