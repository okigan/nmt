import ndp


def test_ndp():
    ndp.init()

    value_in = 5
    oid = ndp.put(value_in)
    value_out = ndp.get(oid)

    assert value_in == value_out
