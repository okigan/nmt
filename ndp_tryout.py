import ndp

@ndp.remote
def f():
    return 5

def main():
    ndp.init()

    ndp.put(f())

    print(f())


main()
