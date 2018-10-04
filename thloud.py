import base64
import pickle


def sum_to(to, label='hello'):
    total = 0
    while to > 0:
        total += to
        to -= 1
    return label, total


# pool = multiprocessing.Pool()

import boto3

_sqs = boto3.client('sqs')
_queue_name = 'thloud'
_queue_url = None

_results = {}


class CloudTask(object):
    def __init__(self, message_id) -> None:
        super().__init__()
        self.message_id = message_id

    def get(self):

        while True:
            if self.message_id in _results:
                return _results[self.message_id]

            result = _sqs.receive_message(QueueUrl=_queue_url)

            try:
                for message in result['Messages']:
                    message_id = message['MessageId']
                    payload: str = message['Body']

                    load = pickle.loads(base64.decodestring(payload.encode()))
                    func, args, kwds = load['func'], load['args'], load['kwds']
                    _results[message_id] = func(*args)
                    _sqs.delete_message(QueueUrl=_queue_url, ReceiptHandle=message['ReceiptHandle'])
            finally:
                pass


class CloudPool(object):
    def __init__(self) -> None:
        super().__init__()
        _sqs = boto3.client('sqs')
        _queue_name = 'thloud'
        try:
            result = _sqs.get_queue_url(QueueName=_queue_name)
            global _queue_url
            _queue_url = result['QueueUrl']
        except Exception as e:
            _sqs.create_queue(QueueName=_queue_name, Attributes={})
            _queue_url = _sqs.get_queue_url(QueueName=_queue_name)

    def apply_async(self, func, args=(), kwds={}, callback=None, error_callback=None):
        dumps = pickle.dumps({"func": func, "args": args, "kwds": kwds})
        encodebytes = base64.encodebytes(dumps)
        payload = str(encodebytes, "utf-8")

        send_message_result = _sqs.send_message(QueueUrl=_queue_url, MessageBody=payload, DelaySeconds=123)
        message_id: str = send_message_result['MessageId']

        return CloudTask(message_id)
        print(6)


pool = CloudPool()


def span(func, *args, **kwargs):
    aresult = pool.apply_async(func, (*args,), kwds={**kwargs})
    return aresult


class Loop(object):

    def run_forever(self):
        pass


if __name__ == '__main__':
    loop = Loop()

    # pool = multiprocessing.Pool()
    N = 100_000

    aresult1 = span(sum_to, N, label='yin')
    aresult2 = span(sum_to, N, label='yen')

    print(aresult1.get())
    print(aresult2.get())
