# ffprobe -show_frames -print_format json
import json
import os
import shutil
import sys
import tempfile
# import threading
import time
import urllib.parse
from typing import *

import boto3
# import fire
import fire
import ray
import smart_open

import nmt_timeit
import util
from util import manifiq_url, get_bucket, get_key, get_s3client



def echo(e):
    return e


class FrameData:
    # expand with https://blog.mosthege.net/2016/11/12/json-deserialization-of-nested-objects/
    frames: List[object]

    def __init__(self, d):
        self.__dict__ = d


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


def concatenate(sources: Union[List[str], List[Source], Iterator[str]], destination):
    if smart_exists(destination):
        log("skipping concatenate as output already exists")
        return 0

    import tempfile
    destination_tmp = tempfile.mktemp('', prefix=util.get_curr_function_name())

    if isinstance(sources, List) and all(isinstance(elem, str) for elem in sources):
        sources = [Source(s) for s in sources]


    lines = []
    for source in sources:
        if isinstance(source, str):
            lines += [f"file '{manifiq_url(source)}'"]
        elif isinstance(source, Source):
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
        # f'-protocol_whitelist file,http,https,tcp,tls -i {list_file_path} ' #for ffmpeg 4.+
        f'-i {list_file_path} '
        f'-c copy -f mp4 {destination_tmp} 1>{concat_stdout} 2>{concat_stderr}')

    smart_move(destination_tmp, destination)

    return return_code


def smart_exists(url) -> bool:
    result = urllib.parse.urlparse(url)
    if result.scheme in ['', 'file']:
        return os.path.exists(url)
    elif result.scheme == 's3':
        try:
            response = get_s3client().head_object(Bucket=get_bucket(result), Key=get_key(result))
            size = response['ContentLength']
            return size > 0
        except Exception as e:
            return False


def smart_delete(url):
    result = urllib.parse.urlparse(url)
    if result.scheme in ['', 'file']:
        return os.remove(url)
    elif result.scheme == 's3':
        response = get_s3client().delete_object(Bucket=get_bucket(result), Key=get_key(result))


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


def smart_size(url):
    result = urllib.parse.urlparse(url)
    if result.scheme in ['', 'file']:
        return os.path.getsize(url)
    elif result.scheme == 's3':
        try:
            response = get_s3client().head_object(Bucket=get_bucket(result), Key=get_key(result))
            size = response['ContentLength']
            return size > 0
        except Exception as e:
            return False


@nmt_timeit.ftimer
def smart_move(source, destination):
    result = urllib.parse.urlparse(destination)

    if result.scheme in ['', 'file']:
        with smart_open.smart_open(source, 'rb') as src:
            with smart_open.smart_open(destination, 'wb') as dst:
                shutil.copyfileobj(src, dst)
    elif result.scheme == 's3':

        # get_s3client().upload_file(source, get_bucket(result), get_key(result))
        transfer = boto3.s3.transfer.S3Transfer(get_s3client())
        transfer.upload_file(source, get_bucket(result), get_key(result), callback=ProgressPercentage(smart_size(source), source))
        os.remove(source)


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
               f'-c:v libx264 -preset ultrafast -strict -2 {chunk_file_tmp} '
               f'1>{chunk_file_stdout} '
               f'2>{chunk_file_stderr}')
    util.run(command)

    smart_move(chunk_file_tmp, destination)

    return destination


def encode_chunks(source, chunks):
    enumerated_chunks = enumerate(chunks)
    n = len(chunks)

    # chunk_files = map(lambda o: encode_chunk_main(o[1], o[0], n, source), list(enumerated_chunks))

    chunk_files = raymap(lambda o: encode_chunk_main(o[1], o[0], n, source), list(enumerated_chunks))

    # chunk_files = []
    # for i, c in enumerated_chunks:
    #     # oid = encode_chunk_main.remote(c, i, n, source)
    #     oid = add2.remote(5,6)
    #     val = ray.get(oid)
    #     chunk_files += [val]
    #
    return chunk_files


def encode_chunk_main(chunk, i, n, source):
    chunk_file = generate_intermedite_object_path(f'chunk{i}.mov', [i, n, source, 'v6'])
    start_time = float(chunk[0]['best_effort_timestamp_time'])
    duration = float(chunk[1]['best_effort_timestamp_time']) - start_time
    return encode_chunk(chunk_file, source, start_time, duration, i, n)


def hash(input: object):
    return util.md5(json.dumps(input, ensure_ascii=False).encode('utf8'))


def generate_intermedite_object_path(s: str, o: object):
    suffix = f'{s}_{hash(o)}'
    bucket = 'netflix.s3.genpop.test'
    prefix = 'mce/temp/maple_exp/intermediate_obj'
    return os.path.join("s3://", bucket, prefix, suffix)


@ray.remote
def step(func, arg):
    return func(arg)


def raymap(func, iter):
    oids = [step.remote(func, i) for i in iter]
    vals = [ray.get(oid) for oid in oids]

    return vals


def analyze_source(url: str) -> FrameData:
    murl = manifiq_url(url)

    final_name = "frames.json"

    if not smart_exists(final_name):
        stdout = tempfile.mktemp("frames_out")
        stderr = tempfile.mktemp("frames_err")
        command = f"ffprobe -show_frames -print_format json '{murl}' 1>{stdout} 2>{stderr}"
        util.run(command)
        shutil.move(stdout, final_name)

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
    def transcode(self, source: str, destination: str, trim_start_sec=None, trim_duration_sec=None):
        transcode(source,
                  destination,
                  trim_start_sec=trim_start_sec,
                  trim_duration_sec=trim_duration_sec)


def x(url):
    result = urllib.parse.urlparse(url)
    return get_s3client().generate_presigned_url(ClientMethod='put_object',
                                                 Params={'Bucket': (get_bucket(result)), 'Key': (get_key(result))})


# print(x('s3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/output/s3_put.mp4'))
def main():
    fire.Fire(Nmt)


@ray.remote
def add2(a, b):
    return a + b


# python nmt.py transcode
#   --source 's3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/data/bbb_sunflower_1080p_30fps_normal.mp4'
#   --destination 's3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/output/bbb_sunflower_1080p_30fps_normal.mp4'
# #   --trim_start 4
if __name__ == '__main__':
    ray.init()

    # x_id = add2.remote(1, 2)
    # print(ray.get(x_id))

    # x_id = step.remote(lambda x: x*2, 2)
    # print(ray.get(x_id))
    #
    # oids = [step.remote(lambda x: x*2, i) for i in [3,4]]
    # vals = [ray.get(oid) for oid in oids]
    # print(vals)

    # transcode('s3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/data/bbb_sunflower_1080p_30fps_normal.mp4',
    #           's3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/output/bbb_sunflower_1080p_30fps_normal4.mp4')

    main()
