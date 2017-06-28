import redis
import subprocess
#fh is the inode number, it should be used to identify a file. However, the filename cannot be acquired yet

def init (param):
    return None
def readxform (fh, buf, offset, length):
    # After the original read, we transform the buffer
    if (length>0):
       arr = bytearray(buf)
       #for i in range(len(arr)):
       #   arr[i] = arr[i] # (arr[i] ^ 0xAB)
       buf = bytes(arr)
    return buf

def writexform (fh, buf, offset, length):
    # Before the original write, we transform the buffer  
    if (length>0):
       arr = bytearray(buf)
       #for i in range(len(arr)):
       #   arr[i] = 100 # (arr[i] ^ 0xAB)
       buf = bytes(arr)
    return buf

def readxform_c (self,buf,param):
    if len(self.previous)==0:
       arr = bytearray(buf)
    else:
       arr = bytearray(self.previous+buf)
#       print ('Reused previous ',len(self.previous), len (buf), len (arr))
       self.previous = b''
    index = 0
    if (len(buf)<4):
       self.previous = buf
       return None
    if (len(buf)>4 and chr(arr[0]) == 'C' and chr(arr[1]) =='O' and chr(arr[2])=='M' and chr(arr[3])=='P'): 
       index = 4
       num = 10
       if (len(buf) < index+num):
          self.previous = buf
          return None
       size = int.from_bytes(bytes(arr[index:index+num]),byteorder='big')
 #      print('Size of record: ',size, ' provided ', len(buf), self.fh.__dict__)
       cparr = arr[index+num:]   # We copy it as we may send it to self.previous
       if (len(cparr) < size):
          needed = size-len(cparr)
  #        print ('Reading more data', needed)
          buf2 = self.fh.read(needed)
          if (len(buf2) < needed):
 #            print ('Not available')
             if buf2:
 #               print ('Rescued ', len(buf2))
                cparr = cparr + bytearray(buf2)
                buf2 = None
          elif (len(buf2) > needed): # With compression (or other filters) we may get more data than the asked.
               cparr = cparr + bytearray(buf2[:needed])
               buf2 = buf2[needed:]
 #              print ('Storing data for later use as previous filter offered more data than requested', len(buf2))
          else: # We have the exact size
             cparr = cparr + bytearray(buf2)
             buf2 = None
       elif (len(cparr) > size):
          buf2 = bytes(cparr[size:])
          cparr = cparr[:size]
  #        print ('Storing data for later use', len(buf2))
       else:
          buf2 = None
 #         print('Exact Data')
 #      print ('Processing with ',len(cparr))
       if (size != len(cparr)): # If after all that we tried we do not have enough date, expect the next object would have it
          if not buf2:
             self.previous = bytes(arr[:index+num])+bytes(cparr)
          else:
             self.previous = bytes(arr[:index+num])+bytes(cparr)+buf2
 #         print ('Return none')
          return None
       arr = cparr
       p = subprocess.Popen(["/usr/bin/gzip","-c","-d"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
       #print(arr)
       bufx, err = p.communicate(input=bytes(arr))
       if not buf2:
          return bufx
       else:
          bufx = bufx
          temp = readxform_c(self, buf2, param)
          if temp:
             bufx = bufx + temp
       return bufx
    else:
        print ('Not found COMP')
    buf = bytes(arr)
    return buf

def writexform_c (self,buf,param):
    arr = bytearray(buf)
    header = "COMP"
    params = param.split()
    redis_host = str(params[0]).lower()
    redis_port =  int(params[1])
    r = redis.Redis(connection_pool=redis.ConnectionPool(host=redis_host, port=redis_port, db=0))
    compression = r.get(params[2])
    if (not compression == b'off'):
       p = subprocess.Popen(["/usr/bin/gzip","-c"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
       arr, err = p.communicate(input=buf)
       print (len(buf), len(arr),err, self.fh.__dict__)
       arr = bytearray(header.encode('ascii'))+bytearray((len(arr)).to_bytes(10,byteorder='big'))+bytearray(arr)
    buf = bytes(arr)
    return buf


def flush(self,param):
    buf = None
#    print ('Flushing')
    try:
       print(self.fh.fh.name)
    except:
       return buf
    return buf
