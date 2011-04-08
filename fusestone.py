#!/usr/bin/env python

from StringIO import StringIO
from pprint import pprint
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
        self.msg = {}
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

        if path in self.files:
            st.st_mode = stat.S_IFREG | 0644
            st.st_size = 1024*1024
            return st

        return -errno.ENOENT

    def _readdir_ks(self, type_, objs, name_key=None, prefix=None):
        if not name_key:
            name_key = 'name'
        if not prefix:
            prefix = '/'
        ret = []

        if type_ == 'result':
            objs = objs['data']['results']
        else:
            objs = objs['data']

        for obj in objs:
            log(obj)
            d = obj[name_key]
            collection = self.dirs
            if type_ == 'result':
                d = obj['values'][3] + ' ' + d
                collection = self.files
            p = prefix + '/' + d
            collection[p] = type_, obj
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
            if type_ == 'filter':
                f = self.get_parent(path)
                _ = self.get_parent(f)
                p = self.get_parent(_)
                log(p)
                _, f = self.dirs[f]
                _, p = self.dirs[p]
                objs = self._readdir_ks('result', self.ks.get_results(p['id'], obj['id']), prefix=path,
                    name_key='message_id')

            if objs:
                log(len(objs))
                for o in objs:
                    yield o

    def open(self, path, flags):
        if path not in self.files:
            return -errno.ENOENT
        type_, obj = self.files.get(path, None)
        p = self.get_parent(path)
        _ = self.get_parent(p)
        _ = self.get_parent(_)
        _ = self.get_parent(_)
        _, p = self.dirs[_]
        self.msg[path] = self.ks.get_message(p['id'], obj['message_id'])
        log(self.msg)

    def read(self, path, size, offset):
        if path not in self.files:
            return -errno.ENOENT
        f = StringIO()
        pprint(self.msg[path], f)
        msg = f.getvalue()
        return msg[offset:offset + size]


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

