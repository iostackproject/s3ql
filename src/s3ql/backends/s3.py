'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from .common import AbstractConnection, AbstractBucket
from time import sleep
from boto.s3.connection import S3Connection, Location
from contextlib import contextmanager
import boto.exception as bex
from s3ql.common import (TimeoutError, QuietError)
import logging
import errno
import time

log = logging.getLogger("backend.s3")

class Connection(AbstractConnection):
    """Represents a connection to Amazon S3

    This class just dispatches everything to boto. Note separate boto connection 
    object for each thread.
    """

    def __init__(self, awskey, awspass):
        super(Connection, self).__init__()
        self.awskey = awskey
        self.awspass = awspass
        self.pool = list()
        self.conn_cnt = 0

    def _pop_conn(self):
        '''Get boto connection object from the pool'''

        try:
            conn = self.pool.pop()
        except IndexError:
            # Need to create a new connection
            log.debug("Creating new boto connection (active conns: %d)...",
                      self.conn_cnt)
            conn = S3Connection(self.awskey, self.awspass)
            self.conn_cnt += 1

        return conn

    def _push_conn(self, conn):
        '''Return boto connection object to pool'''
        self.pool.append(conn)

    def delete_bucket(self, name, recursive=False):
        """Delete bucket"""

        if not recursive:
            with self._get_boto() as boto:
                boto.delete_bucket(name)
                return

        # Delete recursively
        with self._get_boto() as boto:
            step = 1
            waited = 0
            while waited < 600:
                try:
                    boto.delete_bucket(name)
                except bex.S3ResponseError as exc:
                    if exc.code != 'BucketNotEmpty':
                        raise
                else:
                    return
                self.get_bucket(name, passphrase=None).clear()
                time.sleep(step)
                waited += step
                step *= 2

            raise RuntimeError('Bucket does not seem to get empty')


    @contextmanager
    def _get_boto(self):
        """Provide boto connection object"""

        conn = self._pop_conn()
        try:
            yield conn
        finally:
            self._push_conn(conn)

    def create_bucket(self, name, passphrase=None):
        """Create and return an S3 bucket
        
        Note that a call to `get_bucket` right after creation may fail,
        since the changes do not propagate instantaneously through AWS.
        """

        with self._get_boto() as boto:
            # We need an EU bucket for the list-after-put consistency,
            # otherwise it is possible that we read old metadata
            # without noticing it.
            try:
                boto.create_bucket(name, location=Location.EU)
            except bex.S3ResponseError as exc:
                if exc.code == 'InvalidBucketName':
                    log.error('Bucket name contains invalid characters.')
                    raise QuietError(1)
                else:
                    raise

        return Bucket(self, name, passphrase)

    def get_bucket(self, name, passphrase=None):
        """Return a bucket instance for the bucket `name`
        
        Raises `KeyError` if the bucket does not exist.
        """

        with self._get_boto() as boto:
            try:
                boto.get_bucket(name)
            except bex.S3ResponseError as e:
                if e.status == 404:
                    raise KeyError("Bucket %r does not exist." % name)
                else:
                    raise
        return Bucket(self, name, passphrase)

class Bucket(AbstractBucket):
    """Represents a bucket stored in Amazon S3.

    This class should not be instantiated directly, but using
    `Connection.get_bucket()`.

    Due to AWS' eventual propagation model, we may receive e.g. a 'unknown bucket'
    error when we try to upload a key into a newly created bucket. For this reason,
    many boto calls are wrapped with `retry_boto`. Note that this assumes that
    no one else is messing with the bucket at the same time.
    """

    @contextmanager
    def _get_boto(self):
        '''Provide boto bucket object'''
        # Access to protected methods ok
        #pylint: disable-msg=W0212

        boto_conn = self.conn._pop_conn()
        try:
            yield retry_boto(boto_conn.get_bucket, self.name)
        finally:
            self.conn._push_conn(boto_conn)

    def __init__(self, conn, name, passphrase):
        super(Bucket, self).__init__()
        self.conn = conn
        self.passphrase = passphrase
        self.name = name

    def __str__(self):
        if self.passphrase:
            return '<encrypted s3 bucket, name=%r>' % self.name
        else:
            return '<s3 bucket, name=%r>' % self.name

    def contains(self, key):
        with self._get_boto() as boto:
            bkey = retry_boto(boto.get_key, key)

        return bkey is not None

    def raw_lookup(self, key):
        with self._get_boto() as boto:
            bkey = retry_boto(boto.get_key, key)

        if bkey is None:
            raise KeyError('Key does not exist: %s' % key)

        return bkey.metadata

    def delete(self, key, force=False):
        """Deletes the specified key

        ``bucket.delete(key)`` can also be written as ``del bucket[key]``.
        If `force` is true, do not return an error if the key does not exist.
        """

        if not isinstance(key, str):
            raise TypeError('key must be of type str')

        with self._get_boto() as boto:
            if not force and retry_boto(boto.get_key, key) is None:
                raise KeyError('Key does not exist: %s' % key)

            retry_boto(boto.delete_key, key)

    def list(self, prefix=''):
        """List keys in bucket

        Returns an iterator over all keys in the bucket.
        """

        with self._get_boto() as boto:
            for bkey in boto.list(prefix):
                yield bkey.name

    def get_size(self):
        """Get total size of bucket"""

        with self._get_boto() as boto:
            size = 0
            for bkey in boto.list():
                size += bkey.size

        return size

    def raw_fetch(self, key, fh):
        with self._get_boto() as boto:
            bkey = retry_boto(boto.get_key, key)
            if bkey is None:
                raise KeyError('Key does not exist: %s' % key)
            fh.seek(0)
            retry_boto(bkey.get_contents_to_file, fh)

        return bkey.metadata

    def raw_store(self, key, fh, metadata):
        with self._get_boto() as boto:
            bkey = boto.new_key(key)
            bkey.metadata.update(metadata)
            retry_boto(bkey.set_contents_from_file, fh)


    def copy(self, src, dest):
        """Copy data stored under `src` to `dest`"""

        if not isinstance(src, str):
            raise TypeError('key must be of type str')

        if not isinstance(dest, str):
            raise TypeError('key must be of type str')

        with self._get_boto() as boto:
            retry_boto(boto.copy_key, dest, self.name, src)

def retry_boto(fn, *a, **kw):
    """Wait for fn(*a, **kw) to succeed
    
    If `fn(*a, **kw)` raises `boto.exception.S3ResponseError` with errorcode
    in (`NoSuchBucket`, `RequestTimeout`) or `IOError` with errno 104,
    the function is called again. If the timeout is reached, 
    `TimeoutError` is raised.
    """

    step = 0.2
    timeout = 300
    waited = 0
    while waited < timeout:
        try:
            return fn(*a, **kw)
        except bex.S3ResponseError as exc:
            if exc.error_code in ('NoSuchBucket', 'RequestTimeout'):
                pass
            else:
                raise
        except IOError as exc:
            if exc.errno == errno.ECONNRESET:
                pass
            else:
                raise

        sleep(step)
        waited += step
        if step < timeout / 30:
            step *= 2

    raise TimeoutError()