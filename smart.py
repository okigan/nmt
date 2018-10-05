import os
import shutil
import sys
import time
import urllib.parse

import smart_open

import nmt_timeit
from util import get_bucket, get_key, get_s3client_for_bucket


def smart_etag(url) -> str:
    result = urllib.parse.urlparse(url)
    if result.scheme in ['', 'file']:
        return os.path.getmtime(url)
    elif result.scheme == 's3':
        try:
            response = get_s3client_for_bucket(url).head_object(Bucket=get_bucket(result), Key=get_key(result))
            size = response['ETag'][1:-1]
            return size > 0
        except Exception as e:
            return False


def smart_exists(url) -> bool:
    result = urllib.parse.urlparse(url)
    if result.scheme in ['', 'file']:
        return os.path.exists(url)
    elif result.scheme == 's3':
        return smart_size(url) > 0


def smart_delete(url):
    result = urllib.parse.urlparse(url)
    if result.scheme in ['', 'file']:
        return os.remove(url)
    elif result.scheme == 's3':
        response = get_s3client_for_bucket(url).delete_object(Bucket=get_bucket(result), Key=get_key(result))


def smart_size(url):
    result = urllib.parse.urlparse(url)
    if result.scheme in ['', 'file']:
        return os.path.getsize(url)
    elif result.scheme == 's3':
        try:
            response = get_s3client_for_bucket(url).head_object(Bucket=get_bucket(result), Key=get_key(result))
            size = response['ContentLength']
            return size > 0
        except Exception as e:
            return False


class ProgressPercentage(object):
    KB = 1024
    MB = KB * 1024

    def __init__(self, size, desc=None):
        self.desc = desc
        self.size = size
        self.seen_so_far = 0
        self.startTs = time.time()
        self.updatedTs = 0

    def __call__(self, bytes_amount):
        self.seen_so_far += bytes_amount
        now = time.time()
        if now - self.updatedTs > 5:
            speedMBPs = self.method_name(now)
            percent = 100 * self.seen_so_far / (1.0 * self.size) if self.size != 0 else '-'
            sys.stdout.write('{} / {} = {}% @ {} MB/s for {}\n'.format(self.seen_so_far, self.size, percent, speedMBPs, self.desc))
            sys.stdout.flush()
            self.updatedTs = now

    def method_name(self, now):
        return (self.seen_so_far / self.MB) / (now - self.startTs)


@nmt_timeit.ftimer
def smart_move(source, destination):
    result = urllib.parse.urlparse(destination)

    if result.scheme in ['', 'file']:
        with smart_open.smart_open(source, 'rb') as src:
            with smart_open.smart_open(destination, 'wb') as dst:
                shutil.copyfileobj(src, dst)
    elif result.scheme == 's3':
        get_s3client_for_bucket(destination).upload_file(source,
                                                         get_bucket(result),
                                                         get_key(result),
                                                         Callback=ProgressPercentage(smart_size(source), source))
        os.remove(source)
