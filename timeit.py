import time


def timeit(method):
    def timed(*args, **kw):
        func = '%s(%s' % (method.__name__, ''.join(*args))

        print('running: {0}'.format(func))
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%s  %2.2f ms' % (func, (te - ts) * 1000))
        return result

    return timed
