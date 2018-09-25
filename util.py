import hashlib
import os
import time


def make_dir(working_dir):
    try:
        os.makedirs(working_dir)
    except OSError:
        pass

    return working_dir


def run(command):
    start = time.time()
    print('running: {0}'.format(command))
    return_code = os.system(command)
    print('return code: {0} in {1} seconds from {2}'.format(return_code, time.time() - start, command))

    return return_code


def create_working_dir(path: str) -> str:
    md = hashlib.md5()
    md.update(path.encode('utf-8'))
    working_dir = md.hexdigest()

    return make_dir(working_dir)
