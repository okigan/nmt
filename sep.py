import ray

from nmt import raymap, encode_chunk_main


@ray.remote
def add2(a, b):
    return a + b

