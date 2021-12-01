#!/usr/bin/python
import json
import csv


def load_json(fname):
    with open(fname) as fd:
        p = json.load(fd)

    return p

def enc(row, key):
    v = row[key]
    if not isinstance(v, basestring):
        v = str(v)

    return v.encode('utf-8')
 
def save(outfile, lines, header):
    with open(outfile, 'w') as f:
        writer = csv.writer(f, delimiter=',')

        writer.writerow(header)

        for r in lines:

            #writer.writerow([r[k] for k in header])
            writer.writerow([enc(r, k) for k in header])


import sys

fname = sys.argv[1]
outfile = fname + '.csv'

data = load_json(fname)

header = data[0].keys()

print 'header:', header
print 'outfile:', outfile
 
save(outfile, data, header)
