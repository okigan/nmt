import uuid as uuid


def dmap(func, iter):
    return map(func, iter)


class ObjectId(object):
    def __init__(self, object_id) -> None:
        super().__init__()
        self.object_id = object_id


class Ndp(object):
    def __init__(self) -> None:
        super().__init__()
        self._values = {}

    def put(self, value):
        oid = uuid.uuid4()
        self._values[oid] = value
        return ObjectId(oid)

    def get(self, object_id: ObjectId):
        return self._values.get(object_id.object_id)

    def init(self):
        pass


__g_ndp = Ndp()


def init():
    return __g_ndp.init()


def put(value: object):
    return __g_ndp.put(value)


def get(id: ObjectId):
    return __g_ndp.get(id)


def remote(method):
    def remoted_function(*args, **kw):
        return method(*args, **kw)

    return remoted_function
