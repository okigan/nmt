# ffprobe -show_frames -print_format json
# import threading

import boto3
# import fire
import ray


@ray.remote
def add2(a, b):
    return a + b

@ray.remote
def step(func, *args):
    return func(*args)

@ray.remote
def list_buckets():
    return get_s3().list_buckets()


def get_s3():
    return boto3.client('s3', region_name='us-west-2')

# python nmt.py transcode
#   --source 's3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/data/bbb_sunflower_1080p_30fps_normal.mp4'
#   --destination 's3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/output/bbb_sunflower_1080p_30fps_normal.mp4'
# #   --trim_start 4
if __name__ == '__main__':
    ray.init()

    x_id = add2.remote(1, 2)
    print(ray.get(x_id))

    x_id = step.remote(lambda x: x * 2, 3)
    print(ray.get(x_id))

    oids = [step.remote(lambda x, y, z, w: x * 2, i, 1, 2, 3) for i in [3, 4]]
    vals = [ray.get(oid) for oid in oids]
    print(vals)

    oids = [list_buckets.remote()]
    vals = [ray.get(oid) for oid in oids]
    print(vals)

    # transcode('s3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/data/bbb_sunflower_1080p_30fps_normal.mp4',
    #           's3://us-west-2.netflix.s3.genpop.test/mce/temp/maple_exp/output/bbb_sunflower_1080p_30fps_normal.mp4')

    # main()
