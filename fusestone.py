#!/usr/bin/env python

import json
import sys
import os
import stat
import errno

try:
    import _find_fuse_parts
except ImportError:
    pass
import fuse
from fuse import Fuse

import keystone


ROOT = os.path.abspath(os.path.dirname(__file__))

if not hasattr(fuse, '__version__'):
    raise RuntimeError, \
        "your fuse-py doesn't know of fuse.__version__, probably it's too old."

fuse.fuse_python_api = (0, 2)

hello_path = '/hello'
hello_str = 'Hello World!\n'

class MyStat(fuse.Stat):
    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 0
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0

def log(msg):
    open(os.path.join(ROOT, 'fusestone.log'), 'a+').write(str(msg) + '\n')

def wrap(func):
    def catch(*args, **kw):
        try:
            log(' -> ' + func.func_name + ' with ' + str(args[1:]) + ' ' + str(kw))
            return func(*args, **kw)
        except Exception, e:
            log('exception!')
            log(e)
    return catch


class Fusestone(Fuse):

    def __init__(self, *args, **kw):
        self.dirs = {}
        self.files = {}
        log('Fusestone Starting...')
        self.config = kw['config']
        self.ks = keystone.API(self.config['host'])
        print self.ks.login(self.config['user'], self.config['pass'])
        del kw['config']
        Fuse.__init__(self, *args, **kw)

    @wrap
    def getattr(self, path):
        st = MyStat()

        if path == '/':
            st.st_mode = stat.S_IFDIR | 0755
            st.st_nlink = 2
            return st
        
        if path in self.dirs:
            st.st_mode = stat.S_IFDIR | 0755
            st.st_nlink = 0
            return st

#        elif path == hello_path:
#            st.st_mode = stat.S_IFREG | 0444
#            st.st_nlink = 1
#            st.st_size = len(hello_str)
#        else:

        return -errno.ENOENT
   
    @wrap
    def readdir(self, path, offset):
        yield fuse.Direntry('.')
        yield fuse.Direntry('..')
        if path == '/':
            self.projects = self.ks.get_projects()['data']
            for project in self.projects:
                d = str(project['short_name'])
                self.dirs['/' + d] = project
                yield fuse.Direntry(d)

    def open(self, path, flags):
        if path != hello_path:
            return -errno.ENOENT
        accmode = os.O_RDONLY | os.O_WRONLY | os.O_RDWR
        if (flags & accmode) != os.O_RDONLY:
            return -errno.EACCES

    def read(self, path, size, offset):
        if path != hello_path:
            return -errno.ENOENT
        slen = len(hello_str)
        if offset < slen:
            if offset + size > slen:
                size = slen - offset
            buf = hello_str[offset:offset+size]
        else:
            buf = ''
        return buf

def main():

    config = json.load(open('config.json'))
    
    usage="""
Keystone Fuse
""" + Fuse.fusage
    server = Fusestone(
        version="%prog " + fuse.__version__,
        usage=usage,
        dash_s_do='setsingle',
        config=config)

    server.parse(errex=1)
    server.main()

if __name__ == '__main__':
    main()

