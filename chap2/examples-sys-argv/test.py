import sys

if __name__ == '__main__':
    print('total number of args: ', len(sys.argv))
    for elem in sys.argv:
        t = type(elem)
        print(f'elem = {elem} with type {t}')
