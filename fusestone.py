#!/usr/bin/env python

import re
import json
import sys
import os
import stat
import errno
import traceback

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

def logpointer():
    return open(os.path.join(ROOT, 'fusestone.log'), 'a+')

def log(msg):
    logpointer().write(str(msg) + '\n')

def wrap(func):
    def catch(*args, **kw):
        log(' -> ' + func.func_name + ' with ' + \
            str(args[1:]) + ' ' + str(kw))
        return func(*args, **kw)
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
            st.st_nlink = 2
            return st

        return -errno.ENOENT

    def _readdir_ks(self, type_, objs, name_key=None, prefix=None):
        log('objs')
        log(objs)
        if not name_key:
            name_key = 'name'
        if not prefix:
            prefix = '/'
        ret = []
        for obj in objs['data']:
            d = obj[name_key]
            p = prefix + '/' + d
            self.dirs[p] = type_, obj
            log(d)
            ret.append(fuse.Direntry(str(d)))
        return ret

    def get_parent(self, f):
        return os.path.dirname(f)

    @wrap
    def readdir(self, path, offset):
        yield fuse.Direntry('.')
        yield fuse.Direntry('..')
        if path == '/':
            for project in self.ks.get_projects()['data']:
                d = str(project['short_name'])
                self.dirs['/' + d] = 'project', project
                yield fuse.Direntry(d)
            return
        type_, obj = self.dirs.get(path, None)
        objs = None
        log(obj)
        if obj:
            if type_ == 'project':
                objs = self._readdir_ks('blockheader',
                    self.ks.get_blockheaders(obj['id']), prefix=path)
            if type_ == 'blockheader':
                p = self.get_parent(path)
                _, p = self.dirs[p]
                objs = self._readdir_ks('formtypeheader', self.ks.get_formtypeheaders(
                    p['id'], obj['id']), prefix=path)
            if type_ == 'formtypeheader':
                bh = self.get_parent(path)
                p = self.get_parent(bh)
                _, bh = self.dirs[bh]
                _, p = self.dirs[p]
                objs = self._readdir_ks('filter', self.ks.get_filters(
                    p['id'], bh['id'], obj['id']), prefix=path)
            if type_ == 'result':
                objs = self._readdir_ks('result', obj.results(), prefix=path,
                    name_key='message_id')

            if objs:
                log(len(objs))
                for o in objs:
                    yield o

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

