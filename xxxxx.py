import thloud


def sum_to(to):
    total = 0
    while to > 0:
        total += to
        to -= 1
    return total


def x():
    N = 100_000
    span1 = thloud.spawn(sum_to, N)
    span2 = thloud.spawn(sum_to, N)
    result1 = span1.join()
    result2 = span2.join()
    print(result1, result2)

    assert result1 == 5000050000

    return result1 == result2