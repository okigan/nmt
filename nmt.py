# ffprobe -show_frames -print_format json

import hashlib
import json
import os
import time
from shutil import copyfile
from urllib.parse import urlparse

import boto3

s3 = boto3.client('s3')


def f() -> int:
    return 1

def make_dir(working_dir):
    try:
        os.makedirs(working_dir)
    except OSError:
        pass


def run(command):
    start = time.time()
    print('running: {0}'.format(command))
    return_code = os.system(command)
    print('return code: {0} in {1} seconds from {2}'.format(return_code, time.time() - start, command))

    return return_code


def mount_cloud(source: str):
    result = urlparse(source)
    bucket_name = result.netloc
    key_name = result.path
    key_name = key_name[1:]

    # only useful to reads
    # response = s3.head_object(Bucket=bucket_name, Key=key_name)
    # size = response['ContentLength']
    # print(size)

    prefix = os.path.dirname(key_name)

    local_dir = os.path.join("~/tmp", bucket_name, prefix)
    local_dir = os.path.expanduser(local_dir)
    local_file = os.path.join(local_dir, os.path.basename(key_name))

    if not os.path.exists(local_file):
        make_dir(local_dir)
        command = 'goofys --debug_s3 {0}:/{1} {2}'.format(bucket_name, prefix, local_dir)
        i = run(command)

    return local_file


def get_chunks_from_key_frames(key_frames, min_chunk_duration_seconds):
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


def get_chunks(frames_data):
    frames: [] = frames_data["frames"]
    video_frames = [x for x in frames if x['media_type'] == "video"]
    key_frames = get_key_frames(video_frames)
    chunks = get_chunks_from_key_frames(key_frames, 1 * 60)
    return chunks


def assemble_chunks(chunk_files, destination):
    list_file_path = os.path.join("list.txt")
    with open(list_file_path, "w") as listing_file:
        for chunk_file in chunk_files:
            listing_file.write("file {0}".format(chunk_file))
            listing_file.write('\n')
    return_code = run('ffmpeg -y -f concat -i {0} -c copy {1}'.format(list_file_path, destination))
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
            command = 'ffmpeg -y -i {0} -ss {1} -t {2} -c:v copy {3} 1>{4} 2>{5}'. \
                format(source, start_time, duration, chunk_file_tmp, chunk_file_stdout, chunk_file_stderr)
            return_code = run(command)

            os.rename(chunk_file_tmp, chunk_file)
        chunk_files += [chunk_file]
    return chunk_files


def analyze_source(source):
    frames_data: []
    frames_data_filename = 'frames.json'
    frames_data_path = frames_data_filename
    if not os.path.isfile(frames_data_path):
        frames_file_stderr = 'frames.stderr'

        command = 'ffprobe -show_frames -print_format json {0} 1>{1} 2>{2}'.format(source, frames_data_path, frames_file_stderr)
        return_code = run(command)
    with open(frames_data_path) as s:
        frames_data: [] = json.load(s)
    return frames_data


def create_working_dir(source):
    md = hashlib.md5()
    md.update(source.encode('utf-8'))
    working_dir = md.hexdigest()

    make_dir(working_dir)

    return working_dir


def get_key_frames(video_frames):
    key_frames = []
    prev_frame = video_frames[0]
    for frame in video_frames[1:]:
        if prev_frame['pict_type'] != frame['pict_type']:
            key_frames += [prev_frame]
        prev_frame = frame

    if key_frames[-1] != video_frames[-1]:
        key_frames += [video_frames[-1]]
    return key_frames


# take a source
# define chunks (on 'I' frame boundary and at least 60 seconds)
# encode each chunk
# assemble to output
def transcode(source, destination):
    frames_data = analyze_source(source)

    chunks = get_chunks(frames_data)

    # encode each chunk
    chunk_files = encode_chunks(chunks, source)

    # assemble to output
    return_code = assemble_chunks(chunk_files, destination)
    return return_code


cloud_source_path = "s3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/data/bbb_sunflower_1080p_30fps_normal.mp4"
cloud_destination_path = "s3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/output/bbb_sunflower_1080p_30fps_normal.mp4"


def cloud_transcode(source: str, destination: str):
    working_dir = create_working_dir(source)
    print("working directory {0} for {1}".format(working_dir, source))
    os.chdir(working_dir)

    mounted_cloud_source = mount_cloud(source)
    mounted_cloud_destination = mount_cloud(destination)

    local_destination = 'out.mov'
    transcode(mounted_cloud_source, local_destination)
    copyfile(local_destination, mounted_cloud_destination)


cloud_transcode(cloud_source_path, cloud_destination_path)

# f('/Users/iokulist/Github/okigan/nmt/bbb_sunflower_1080p_30fps_stereo_abl.mp4')
# f('/Users/iokulist/Downloads/IMG_2592.mov')
