import time


def ftimer(method):
    def timed(*args, **kw):
        args_str = ','.join([f'{a}' for a in args])
        kw_str = ','.join([f'{k}={v}' for k, v in kw])
        kw_str = ',' + kw_str if len(kw_str) > 0 else kw_str
        func = f'{method.__name__}({args_str}{kw_str})'

        print(f'running: {func}')
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('completed in %2.2f ms func: %s  ' % ((te - ts) * 1000, func))
        return result

    return timed
