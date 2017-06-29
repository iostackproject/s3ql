import configparser
import importlib
import sys

order = []
filterparams = {}
filterlist = {}
initialized = False

def init ():
    global order
    global filterparams
    global filterlist
    global initialized
    if (initialized == False):
       (order, filterlist, filterparams) = readconf()
       inititialized = True

def readconf (conf = "/tmp/filters.ini"):
    Config = configparser.ConfigParser()
    Config.read(conf)
    order = Config['ORDER']['Order'].split()
    path = Config['ORDER']['Path']
    sys.path.append(path)
    for s_filter in order:
        filterparams[s_filter] = Config[s_filter]['Param']
        filterlist[s_filter] = importlib.import_module(s_filter)
    return (order,filterlist,filterparams)

#fh is the inode number, it should be used to identify a file. However, the filename cannot be acquired yet
def readxform (fh, buf, offset, length):
    # After the original read, we transform the buffer
    init()
    for i in order:
        filterlist[i].init(filterparams[i])
        buf=filterlist[i].readxform(fh,buf,offset,length)
    return buf

def writexform (fh, buf, offset, length):
    # Before the original write, we transform the buffer  
    init()
    reverse_order = reversed(order)
    for i in reverse_order:
        filterlist[i].init(filterparams[i])
        buf=filterlist[i].writexform(fh,buf,offset,length)
    return buf

def objectreadxform (el):
    offset = 0
    while True:
       buf = el.read(BUFSIZE)
       if not buf:
          break
       buf = readxform (NULL,buf,offset,BUFSIZE)
       offset = offset + BUFSIZE
       el.write(buf)
    return el

def objectwritexform (el):
    offset = 0
    while True:
       buf = el.read(BUFSIZE)
       if not buf:
          break
       buf = writexform (NULL,buf,offset,BUFSIZE)
       offset = offset + BUFSIZE
       el.write(buf)
    return el


def inFiltered_outStandard(buf):
    init()
    for i in order:
        filterlist[i].init(filterparams[i])
        buf=filterlist[i].readxform_c(buf)
    return buf    

def inStandard_outFiltered(buf):
    init()
    reverse_order = reversed(order)
    for i in reverse_order:
        filterlist[i].init(filterparams[i])
        buf=filterlist[i].writexform_c(buf)
    return buf

def flush():
    return None
