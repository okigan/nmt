import thloud
from xxxxx import x


def test_baseline():
    spawn = thloud.spawn(x)
    join = spawn.join()
    print(join)

    assert join == True
