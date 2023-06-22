#!/usr/local/bin/python3
import json
import csv


def load_json(fname):
    with open(fname) as fd:
        p = json.load(fd)

    return p

def extract(row, key):
    r = row
    for k in key:
        if k not in r:
            return ''

        r = r[k]

    return r

def select(data, keys):
    out = []
    for row in data:
        r = {}

        for k in keys:
            r['.'.join(k)] = extract(row, k)

        out.append(r)

    return out

def enc(row, key):
    if key not in row:
        return ''

    v = row[key]
    if not isinstance(v, str):
        v = str(v)

    return v #.encode('utf-8')

def save(outfile, lines, header):
    with open(outfile, 'w') as f:
        writer = csv.writer(f, delimiter=';')

        writer.writerow(header)

        for r in lines:

            #writer.writerow([r[k] for k in header])
            writer.writerow([enc(r, k) for k in header])

def usage():
    usg = """
        usage:
            #PROG#  file.json [key1.sub1 key1.sub2 key2 key3.sub1.subsub1 key3.sub1.subsub2 ...]

    """

    print(usg.replace('#PROG#', sys.argv[0].split('/')[-1]))
    sys.exit(0)




import sys

argv = sys.argv[1:]

if len(argv) < 1:
    usage()

fname = argv[0]
keys = argv[1:]
outfile = fname + '.csv'

data = load_json(fname)

if len(keys):
    keys = [k.split('.') for k in keys]
    data = select(data, keys)

header = data[0].keys()

print('header:', header)
print('outfields:', keys)
print('outfile:', outfile)

save(outfile, data, header)
