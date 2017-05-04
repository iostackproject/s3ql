
#fh is the inode number, it should be used to identify a file. However, the filename cannot be acquired yet

def init (param):
    print (param)

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
