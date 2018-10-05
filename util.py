import hashlib
import json
import os
import sys
import time
import urllib
import urllib.parse
from enum import Enum

import boto3


def make_dir(working_dir):
    try:
        os.makedirs(working_dir)
    except OSError:
        pass

    return working_dir


class ExecException(RuntimeError):
    def __init__(self, *args, **kwargs):
        RuntimeError.__init__(self, *args, **kwargs)


def run(command):
    start = time.time()
    print('running: {0}'.format(command))
    return_code = os.system(command)
    print('return code: {0} in {1} seconds from {2}'.format(return_code, time.time() - start, command))

    if return_code not in [0]:
        raise ExecException(return_code, command)

    return return_code


def md5(s: str):
    md5 = hashlib.md5()
    md5.update(s)
    return md5.hexdigest()


def create_working_dir(path: str) -> str:
    working_dir = md5(path)

    return make_dir(working_dir)


def get_curr_function_name():
    prev_frame = sys._getframe(1)
    return prev_frame.f_code.co_name


def get_current_func_code():
    prev_frame = sys._getframe(1)
    return prev_frame.f_code


def load_creds_from_json(path: str):
    with open(path) as f:
        d = json.load(f)

        os.putenv('AWS_ACCESS_KEY_ID', d['access_key_id'])
        os.putenv('AWS_SECRET_ACCESS_KEY', d['secret_access_key'])
        os.putenv('AWS_SESSION_TOKEN', d['session_token'])


class MMode(Enum):
    HTTP = 1
    HTTPS = 2
    MOUNT_SEQUENTIAL = 3
    MOUNT_RANDOM = 4


def schemaOk(schema: str, mmode: MMode):
    modes = {
        "http": frozenset([MMode.HTTP]),
        "https": frozenset([MMode.HTTP]),
        "": frozenset([MMode.MOUNT_SEQUENTIAL]),
        "file": frozenset([MMode.MOUNT_SEQUENTIAL])
    }.get(schema, [])

    return mmode in modes


def get_bucket(result: urllib.parse.ParseResult):
    return result.netloc


def get_key(result: urllib.parse.ParseResult):
    return result.path[1:]


def httpfy_cloud(source: str, s3client) -> str:
    result = urllib.parse.urlparse(source)
    bucket = s3client.get_bucket_location(Bucket=get_bucket(result))
    region = bucket['LocationConstraint']
    s3client = get_s3client(region)

    return s3client.generate_presigned_url(ClientMethod='get_object',
                                           Params={'Bucket': (get_bucket(result)), 'Key': (get_key(result))})


client_cache = {}


def get_s3client(region_name=None):
    client = client_cache.get(region_name, None)

    if client is None:
        client = boto3.client('s3', region_name=region_name)
        client_cache[region_name] = client
    return client


class MountedFile(object):
    _local_file: str

    def __init__(self, local_file, mode) -> None:
        self._local_file = local_file

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if os.path.exists(self._local_file):
            dirname = os.path.dirname(self._local_file)
            run("umount {0}".format(dirname))

    def get_path(self):
        return self._local_file


def mount_cloud(source: str, tmp: str = "~/tmp", mode='r') -> MountedFile:
    if mode == 'r':
        result = urllib.parse.urlparse(source)
        bucket_name = result.netloc
        key_name = result.path[1:]

        # only useful to reads
        # response = s3.head_object(Bucket=bucket_name, Key=key_name)
        # size = response['ContentLength']
        # print(size)

        prefix = os.path.dirname(key_name)

        local_dir = os.path.join(tmp, bucket_name, prefix)
        local_dir = os.path.expanduser(local_dir)
        local_dir = os.path.abspath(local_dir)
        local_file = os.path.join(local_dir, os.path.basename(key_name))

        if not os.path.exists(local_file):
            make_dir(local_dir)
            command = 'goofys --debug_s3 {0}:/{1} {2}'.format(bucket_name, prefix, local_dir)
            i = run(command)

        return MountedFile(local_file, mode)
    elif mode == 'w':
        import tempfile
        mktemp = tempfile.mktemp("temptocloud", dir=tmp)
        return MountedFile(mktemp, mode)


def manifiq_url(url, mode: str = 'r', target: MMode = MMode.HTTP, *args, **kw):
    """
    Magically transform url into desired alternative, ex, S3 path into signed http url
    :param url:
    :param mode:
    :param http:
    :param args:
    :param kw:
    """
    result = urllib.parse.urlparse(url)

    if schemaOk(result.scheme, target):
        return url

    if target == MMode.HTTP:
        if result.scheme == 's3':
            return httpfy_cloud(url, get_s3client())
    elif target == MMode.MOUNT_SEQUENCIAL:
        if result.scheme == 's3':
            return mount_cloud(url)
