# ffprobe -show_frames -print_format json
import json
import os
import shutil
import sys
import tempfile
# import threading
import urllib.parse
from enum import Enum
from typing import *

import boto3
import fire
import smart_open

import ndp
import nmt_timeit
import util

# util.load_creds_from_json('creds.json')

# s3 = boto3.client('s3')
s3client = boto3.client('s3', region_name='us-west-2')


def echo(e):
    return e


class FrameData:
    # expand with https://blog.mosthege.net/2016/11/12/json-deserialization-of-nested-objects/
    frames: List[object]

    def __init__(self, d):
        self.__dict__ = d


class MountedFile(object):
    _local_file: str

    def __init__(self, local_file, mode) -> None:
        self._local_file = local_file

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if os.path.exists(self._local_file):
            dirname = os.path.dirname(self._local_file)
            util.run("umount {0}".format(dirname))

    def get_path(self):
        return self._local_file


def get_bucket(result: urllib.parse.ParseResult):
    return result.netloc


def get_key(result: urllib.parse.ParseResult):
    return result.path[1:]


def httpfy_cloud(source: str) -> str:
    result = urllib.parse.urlparse(source)
    bucket = s3client.get_bucket_location(Bucket=(get_bucket(result)))
    region = bucket['LocationConstraint']

    return s3client.generate_presigned_url(ClientMethod='get_object', Params={'Bucket': (get_bucket(result)), 'Key': (get_key(result))})


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
            util.make_dir(local_dir)
            command = 'goofys --debug_s3 {0}:/{1} {2}'.format(bucket_name, prefix, local_dir)
            i = util.run(command)

        return MountedFile(local_file, mode)
    elif mode == 'w':
        import tempfile
        mktemp = tempfile.mktemp("temptocloud", dir=tmp)
        return MountedFile(mktemp, mode)


def get_chunks_from_key_frames(key_frames: List[Dict], min_chunk_duration_sec: int = None):
    if min_chunk_duration_sec == None:
        min_chunk_duration_sec = 60

    chunks = []
    prev_frame = key_frames[0]
    for curr_frame in key_frames[1:]:
        curr_timestamp = float(curr_frame['best_effort_timestamp_time'])
        prev_timestamp = float(prev_frame['best_effort_timestamp_time'])
        if curr_timestamp - prev_timestamp > min_chunk_duration_sec:
            chunks += [[prev_frame, curr_frame]]
            prev_frame = curr_frame

    chunks += [[prev_frame, key_frames[-1]]]

    return chunks


def compute_chunks(frames_data: FrameData, trim_start_sec=None, trim_duration_sec=None, min_chunk_duration_sec=None):
    frames: List = frames_data.frames
    video_frames = [x for x in frames if x['media_type'] == "video"]
    key_frames = get_key_frames(video_frames, trim_start_sec, trim_duration_sec)
    chunks = get_chunks_from_key_frames(key_frames, min_chunk_duration_sec)
    return chunks


class Source(object):
    _path: str
    _duration: float = None
    _inpoint: float = None
    _outpoint: float = None

    def __init__(self, path) -> None:
        super().__init__()
        self._path = path

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, value):
        self._path = value

    @property
    def inpoint(self):
        return self._inpoint

    @inpoint.setter
    def inpoint(self, value):
        self._inpoint = value

    @property
    def outpoint(self):
        return self._inpoint

    @outpoint.setter
    def outpoint(self, value):
        self._inpoint = value


def log(param):
    print(param)


def concatenate(sources: Union[List[str], List[Source]], destination):
    if smart_exists(destination):
        log("skipping concatenate as output already exists")
        return 0

    if isinstance(sources, List) and all(isinstance(elem, str) for elem in sources):
        sources = [Source(s) for s in sources]

    import tempfile
    destination_tmp = tempfile.mktemp('', prefix=util.get_curr_function_name())

    lines = []
    for source in sources:
        lines += [f"file '{manifiq_url(source.path)}'"]
        lines += [f"inpoint {source.inpoint}" if source.inpoint is not None else ""]
        lines += [f"outpoint {source.outpoint}" if source.outpoint is not None else ""]

    list_file_path = os.path.join("list.txt")
    with open(list_file_path, "w") as listing_file:
        listing_file.writelines('\n'.join(lines))

    concat_stdout = "concat.stdout"
    concat_stderr = "concat.stderr"

    return_code = util.run(
        f'ffmpeg -y -f concat -safe 0 '
        f'-protocol_whitelist file,http,https,tcp,tls -i {list_file_path} '
        f'-c copy -f mp4 {destination_tmp} 1>{concat_stdout} 2>{concat_stderr}')

    smart_move(destination_tmp, destination)

    return return_code


def smart_exists(url) -> bool:
    result = urllib.parse.urlparse(url)
    if result.scheme in ['', 'file']:
        return os.path.exists(url)
    elif result.scheme == 's3':
        try:
            response = s3client.head_object(Bucket=get_bucket(result), Key=get_key(result))
            size = response['ContentLength']
            return size > 0
        except Exception as e:
            return False


def smart_delete(url):
    result = urllib.parse.urlparse(url)
    if result.scheme in ['', 'file']:
        return os.remove(url)
    elif result.scheme == 's3':
        response = s3client.delete_object(Bucket=get_bucket(result), Key=get_key(result))


class ProgressPercentage(object):
    def __init__(self):
        self._seen_so_far = 0
        # self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # with self._lock:
        self._seen_so_far += bytes_amount
        sys.stdout.write(f'{self._seen_so_far}\n')
        sys.stdout.flush()


@nmt_timeit.ftimer
def smart_move(source, destination):
    result = urllib.parse.urlparse(destination)

    if result.scheme in ['', 'file']:
        with smart_open.smart_open(source, 'rb') as src:
            with smart_open.smart_open(destination, 'wb') as dst:
                shutil.copyfileobj(src, dst)
    elif result.scheme == 's3':

        # s3client.upload_file(source, get_bucket(result), get_key(result))
        transfer = boto3.s3.transfer.S3Transfer(s3client)
        transfer.upload_file(source, get_bucket(result), get_key(result), callback=ProgressPercentage())
        os.remove(source)


@nmt_timeit.ftimer
def encode_chunk(destination: str, source: str, start_time_sec: float, duration_sec: float, i: int, n: int):
    print('processing chunk {0}/{1}'.format(i, n))

    if smart_exists(destination):
        log("skipping encode_chunk")
        return destination

    chunk_file_tmp = os.path.join('chunk{0}.tmp.mov'.format(i))
    chunk_file_stdout = os.path.join('chunk{0}.stdout'.format(i))
    chunk_file_stderr = os.path.join('chunk{0}.stderr'.format(i))

    # command = 'ffmpeg -y -i {0} -ss {1} -t {2} -c:v prores -profile:v 3 {3} 1>{4} 2>{5}'. \
    msg = f'chunk {i}/{n} start {start_time_sec}  duration {duration_sec}'

    command = (f"ffmpeg -y -i '{manifiq_url(source)}' "
               f'-ss {start_time_sec} '
               f'-t {duration_sec} '
               f'-vf drawtext="text=\'{msg}\'" '
               f'-c:v libx264 -preset ultrafast {chunk_file_tmp} '
               f'1>{chunk_file_stdout} '
               f'2>{chunk_file_stderr}')
    util.run(command)

    smart_move(chunk_file_tmp, destination)

    return destination


def hash(input: object):
    return util.md5(json.dumps(input, ensure_ascii=False).encode('utf8'))


def generate_intermedite_object_path(s: str, o: object):
    suffix = f'{s}_{hash(o)}'
    bucket = 'us-west-2.netflix.s3.genpop.test'
    prefix = 'mce/temp/maple_exp/intermediate_obj'
    return os.path.join("s3://", bucket, prefix, suffix)


def encode_chunks(source, chunks):
    chunk_files = []
    enumerated_chunks = enumerate(chunks)
    n = len(chunks)
    for i, chunk in enumerated_chunks:
        chunk_file = generate_intermedite_object_path(f'chunk{i}.mov', [i, n, source])

        start_time = float(chunk[0]['best_effort_timestamp_time'])
        duration = float(chunk[1]['best_effort_timestamp_time']) - start_time

        chunk_files += [encode_chunk(chunk_file, source, start_time, duration, i, n)]

    return chunk_files


class MMode(Enum):
    HTTP = 1
    MOUNT_SEQUENTIAL = 2
    MOUNT_RANDOM = 3


def schemaOk(schema: str, mmode: MMode):
    modes = {
        "http": frozenset([MMode.HTTP]),
        "https": frozenset([MMode.HTTP]),
        "": frozenset([MMode.MOUNT_SEQUENTIAL]),
        "file": frozenset([MMode.MOUNT_SEQUENTIAL])
    }.get(schema, [])

    return mmode in modes


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
            return httpfy_cloud(url)
    elif target == MMode.MOUNT_SEQUENCIAL:
        if result.scheme == 's3':
            return mount_cloud(url)


def analyze_source(url: str) -> FrameData:
    murl = manifiq_url(url)

    final_name = "frames.json"

    if not smart_exists(final_name):
        stdout = tempfile.mktemp("frames_out")
        stderr = tempfile.mktemp("frames_err")
        command = f"ffprobe -show_frames -print_format json '{murl}' 1>{stdout} 2>{stderr}"
        util.run(command)
        os.rename(stdout, final_name)

    with open(final_name) as s:
        return FrameData(json.load(s))


def get_key_frames(video_frames, trim_start_sec=None, trim_duration_sec=None):
    if trim_start_sec is not None:
        video_frames = filter(lambda f: float(f['best_effort_timestamp_time']) >= trim_start_sec, video_frames)

    if trim_duration_sec is not None:
        video_frames = filter(lambda f: float(f['best_effort_timestamp_time']) < trim_start_sec + trim_duration_sec, video_frames)

    video_frames = list(video_frames)
    key_frames = list(filter(lambda f: f['pict_type'] == 'I', video_frames))
    key_frames = list(key_frames)

    if key_frames[0]['coded_picture_number'] != video_frames[0]['coded_picture_number']:
        key_frames = [video_frames[0]] + key_frames

    if key_frames[-1]['coded_picture_number'] != video_frames[-1]['coded_picture_number']:
        key_frames = key_frames + [video_frames[-1]]

    return key_frames


# analyze source
# define chunks (on 'I' frame boundary and at least ~60 seconds)
# encode each chunk
# assemble to output
def transcode(source: str, destination: str, trim_start_sec: float = None, trim_duration_sec: float = None, min_chunk_duration_sec=None):
    frames_data = analyze_source(source)

    chunks = compute_chunks(frames_data, trim_start_sec, trim_duration_sec, min_chunk_duration_sec)

    chunk_files = encode_chunks(source, chunks)

    return_code = concatenate(chunk_files, destination)
    return return_code


class Nmt(object):
    def transcode(self, source: str, destination: str, trim_start_sec=20, trim_duration_sec=10):
        transcode(source,
                  destination,
                  trim_start_sec=trim_start_sec,
                  trim_duration_sec=trim_duration_sec)


def x(url):
    result = urllib.parse.urlparse(url)
    return s3client.generate_presigned_url(ClientMethod='put_object', Params={'Bucket': (get_bucket(result)), 'Key': (get_key(result))})


ndp.init()

# print(x('s3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/output/s3_put.mp4'))
def main():
    fire.Fire(Nmt)


# python nmt.py transcode
#   --source 's3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/data/bbb_sunflower_1080p_30fps_normal.mp4'
#   --destination 's3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/output/bbb_sunflower_1080p_30fps_normal.mp4'
# #   --trim_start 4
if __name__ == '__main__':
    main()
