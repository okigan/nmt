# ffprobe -show_frames -print_format json

import json
import os
from shutil import copyfile
from typing import List, Dict
from urllib.parse import urlparse
from urllib.request import urlopen

import boto3
import fire

import util

s3 = boto3.client('s3')


class FrameData:
    # expand with https://blog.mosthege.net/2016/11/12/json-deserialization-of-nested-objects/
    frames: List[object]

    def __init__(self, d):
        self.__dict__ = d


class MountedFile(object):
    local_file: str

    def __init__(self, local_file) -> None:
        self.local_file = local_file

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if os.path.exists(self.local_file):
            dirname = os.path.dirname(self.local_file)
            util.run("umount {0}".format(dirname))

    def get_path(self):
        return self.local_file


def mount_cloud(source: str, tmp: str = "~/tmp") -> MountedFile:
    result = urlparse(source)
    bucket_name = result.netloc
    key_name = result.path
    key_name = key_name[1:]

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

    return MountedFile(local_file)


def get_chunks_from_key_frames(key_frames: List[Dict], min_chunk_duration_seconds: int):
    chunks = []
    prev_frame = key_frames[0]
    for frame in key_frames[1:]:
        timestamp_ = float(frame['best_effort_timestamp_time'])
        effort_timestamp_ = float(prev_frame['best_effort_timestamp_time'])
        if timestamp_ - effort_timestamp_ > min_chunk_duration_seconds:
            chunks += [[prev_frame, frame]]
            prev_frame = frame

    chunks += [[prev_frame, key_frames[-1]]]

    return chunks


def get_chunks(frames_data: FrameData, trim_start_sec=None, trim_duration_sec=None):
    frames: [] = frames_data.frames
    video_frames = [x for x in frames if x['media_type'] == "video"]
    key_frames = get_key_frames(video_frames, trim_start_sec, trim_duration_sec)
    chunks = get_chunks_from_key_frames(key_frames, 1 * 60)
    return chunks


def assemble_chunks(chunk_files, destination):
    list_file_path = os.path.join("list.txt")
    with open(list_file_path, "w") as listing_file:
        for chunk_file in chunk_files:
            listing_file.write("file {0}".format(chunk_file))
            listing_file.write('\n')

    concat_stdout = "concat.stdout"
    concat_stderr = "concat.stderr"

    return_code = util.run(
        'ffmpeg -y -f concat -i {0} -c copy {1} 1>{2} 2>{3}'.format(list_file_path,
                                                                    destination,
                                                                    concat_stdout,
                                                                    concat_stderr))
    return return_code


def encode_chunks(chunks, source):
    chunk_files = []
    enumerated_chunks = enumerate(chunks)
    for i, chunk in enumerated_chunks:
        chunk_file = os.path.join("chunk{0}.mov".format(i))

        if not os.path.isfile(chunk_file):
            print('processing chunk {0}/{1}'.format(i, len(chunks)))
            start_time = float(chunk[0]['best_effort_timestamp_time'])
            duration = float(chunk[1]['best_effort_timestamp_time']) - start_time
            chunk_file_tmp = os.path.join("chunk{0}.tmp.mov".format(i))

            chunk_file_stdout = os.path.join("chunk{0}.stdout".format(i))
            chunk_file_stderr = os.path.join("chunk{0}.stderr".format(i))

            # command = 'ffmpeg -y -i {0} -ss {1} -t {2} -c:v prores -profile:v 3 {3} 1>{4} 2>{5}'. \
            command = 'ffmpeg -y -i {0} -ss {1} -t {2} -vf drawtext="text=\'{3}\'" -c:v libx264 -preset ultrafast {4} 1>{5} 2>{6}'. \
                format(source,
                       start_time,
                       duration,
                       "chunk {0}/{1} start {2}  stop {3}".format(i, len(chunks),
                                                                  chunk[0]['best_effort_timestamp_time'],
                                                                  chunk[1]['best_effort_timestamp_time']),
                       chunk_file_tmp,
                       chunk_file_stdout,
                       chunk_file_stderr)
            return_code = util.run(command)

            if return_code == 0:
                os.rename(chunk_file_tmp, chunk_file)
                chunk_files += [chunk_file]
        else:
            chunk_files += [chunk_file]

    return chunk_files


def analyze_source(source) -> FrameData:
    frames_data_path = 'frames.json'

    if not os.path.isfile(frames_data_path):
        frames_file_stderr = 'frames.stderr'

        command = 'ffprobe -show_frames -print_format json {0} 1>{1} 2>{2}'.format(source, frames_data_path,
                                                                                   frames_file_stderr)
        return_code = util.run(command)

    with open(frames_data_path) as s:
        frames_data = FrameData(json.load(s))
    return frames_data


def get_key_frames(video_frames, trim_start_sec=None, trim_duration_sec=None):
    if trim_start_sec is not None:
        video_frames = list(filter(lambda f: float(f['best_effort_timestamp_time']) >= trim_start_sec, video_frames))

    if trim_duration_sec is not None:
        video_frames = list(
            filter(lambda f: float(f['best_effort_timestamp_time']) < trim_start_sec + trim_duration_sec,
                   video_frames))

    key_frames = list(filter(lambda f: f['pict_type'] == 'I', video_frames))

    if key_frames[0]['coded_picture_number'] != video_frames[0]['coded_picture_number']:
        key_frames = [video_frames[0]] + key_frames

    if key_frames[-1]['coded_picture_number'] != video_frames[-1]['coded_picture_number']:
        key_frames = key_frames + [video_frames[-1]]

    return key_frames


# analyze source
# define chunks (on 'I' frame boundary and at least ~60 seconds)
# encode each chunk
# assemble to output
def transcode(source: str, destination: str, trim_start_sec: float = None, trim_duration_sec: float = None):
    frames_data = analyze_source(source)

    chunks = get_chunks(frames_data, trim_start_sec, trim_duration_sec)

    chunk_files = encode_chunks(chunks, source)

    return_code = assemble_chunks(chunk_files, destination)
    return return_code


class Nmt(object):
    def transcode(self, source: str, destination: str, trim_start_sec=20, trim_duration_sec=10):

        d = os.curdir
        try:
            working_dir = util.create_working_dir(source)
            print("working directory {0} for {1}".format(working_dir, source))
            os.chdir(working_dir)

            with mount_cloud(source, 'mnt') as mounted_cloud_source:
                with mount_cloud(destination, 'mnt') as mounted_cloud_destination:
                    local_destination = 'out.mov'
                    transcode(mounted_cloud_source.get_path(),
                              local_destination,
                              trim_start_sec=trim_start_sec,
                              trim_duration_sec=trim_duration_sec)
                    copyfile(local_destination, mounted_cloud_destination.get_path())
        finally:
            os.chdir(d)

    def creds(self):
        url = os.path.expandvars('https://casserole.itp.netflix.net/keys/awstest_user/${USER}')
        values = json.load(urlopen(url))
        for k, v in values.items():
            os.putenv(k, v)
            print("{}={}".format(k, v))

        return self


# python nmt.py transcode
#   --source "s3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/data/bbb_sunflower_1080p_30fps_normal.mp4"
#   --destination ""s3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/output/bbb_sunflower_1080p_30fps_normal.mp4"
# #   --trim_start 4
if __name__ == '__main__':
    fire.Fire(Nmt)
