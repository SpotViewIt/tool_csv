#!/usr/bin/python3
import csv
import re
import os
import sys
import datetime
import json
#import pprint
from zlib import crc32
from hashlib import md5

#csv.field_size_limit()
#>> 131072  # 128kb
csv.field_size_limit(1024 * 1024)

header = None
filtercache = None

def separator(f):
    h = f.readline()
    c1 = len(h.split(';'))
    c2 = len(h.split(','))
    f.seek(0)

    return ';' if c1 > c2 else ','


def transcode(row, nline):
    global header

    if header == None:
        header = tuple(row)

    r = {}
    for n, val in enumerate(row):
        if n >= len(header):
            if val:
                 print('warning: troppi campi, linea %s' % nline)
                 r['#unknow %s' % n] = val
                 #print(r)
            continue

        r[header[n]] = val

    return r


def showline(n, r, options):
    op = options[0] if len(options) else None

    if op == 'ln':
        print('line %s' % n, r)
    else:
        print('-' * 10, n, '-' * 10)
        done = set()
        for k in header:
            done.add(k)
            print('   %s: %s' % (repr(k), repr(r[k]) if k in r else '<null>'))

        for k in r:
            if k not in done:
                print('   %s:: %s' % (repr(k), repr(r[k]) if k in r else '<null>'))


        print


def showheader(n, r, options):
    if n == 0:
        hr = repr(header)
        print('header(%d) = %s' % (len(header), hr))
        print('header checksum crc32 = %s' % (hex(crc32(hr) % (1<<32))))
        print('header checksum md5 = %s' % md5(hr).hexdigest())


def showcol(n, r, ln, options):
    if len(options) == 0:
        usage()

    cols = '|'.join([r[k] for k in options])

    if ln:
        print(n, cols)
    else:
        print(cols)


def loadfilters():
    envf = {'WHITE': 'v+', 'BLACK': 'v-', 'FWHITE': 'f+', 'FBLACK': 'f-'} 

    filters = {}
    count = 0

    for kenv in envf:
        if kenv in os.environ:
            count += 1
            mode = envf[kenv][1]
            fromfile = envf[kenv][0] == 'f'

            values = os.environ[kenv]
            key, values = values.split('|', 1)

            if key not in filters:
                filters[key] = {}

            if mode not in filters[key]:
                filters[key][mode] = set()

            if fromfile:
                with open(values) as f:
                    print('open file', values)
                    fvalues = f.read().split('\n')

                    for fv in fvalues:
                        fv = fv.strip()
                        if fv:
                            filters[key][mode].add(fv)
            else:
                values = set(values.split('|'))
                for v in values:
                    filters[key][mode].add(v)

            sys.stderr.write("FILTER %s) BY '%s' MODE='%s' SIZE=%s\n" % (count, key, mode, len(filters[key][mode])))

    if count == 0:
        return False

    return filters


def filterby(r):
    global filtercache

    if filtercache == None:
        filtercache = loadfilters()

    if filtercache == False:
        return True

    ok = True
    for k in filtercache:
        v = r[k]
        fl = filtercache[k]

        if '+' in fl:
            ok = ok and v in fl['+']
        if '-' in fl:
            ok = ok and v not in fl['-']

    return ok


def csvline(row, sep, header = None):
    # TODO port to python3
    import cStringIO
    queue = cStringIO.StringIO()
    writer = csv.writer(queue, delimiter=sep)
    if header:
        writer.writerow([row[k] for k in header])
    else:
        writer.writerow(row)

    return queue.getvalue()

def splitby_flush(state, sep):
    outfile = str(state['chunk']).rjust(4, '0')
    outfile += '_' + state['csvfile']

    if 'counter' not in state:
        print('skip flush', outfile, 'manca counter')
        return
 
    if state['counter'] == 0:
        print('skip flush', outfile, 'nessuna linea')
        return

    print('writing file %s ...' % outfile)
    with open(outfile, 'w') as f:
        writer = csv.writer(f, delimiter=sep)

        if state['chunk'] > 0:
            writer.writerow(header)

        for r in state['lines']:
            writer.writerow([r[k] for k in header])

    state['chunk'] += 1
    state['counter'] = 0
    state['lines'] = []   

def splitby(state, nline, row, sep, options):
    chunksize = int(os.environ['SPLITLINES']) if 'SPLITLINES' in os.environ else 20000

    if len(options) == 0:
        usage()

    key = options[0]

    if len(options) > 1:
        chunksize = int(options[1])
    
    if key not in row:
        print('chiave di split primaria %s non trovata sulla riga')
        sys.exit(0)

    if 'chunk' not in state: #nline == 0:
        print('split su chiave primaria %s ogni %s linee' % (key, chunksize))
        state['chunk'] = 0
        state['counter'] = 0
        state['lines'] = []
        


    if state['counter'] > chunksize:
        if state['lines'][-1][key] != row[key]:
            splitby_flush(state, sep)

    state['lines'].append(row)
    state['counter'] += 1

def delcol(state, n, r, options):
    global header

    if len(options) == 0:
        usage()

    if 'header' not in state:
        head = list(header)
        for todel in options:
            if todel not in head:
                print('ERROR: colonna %s non trovata' % todel)
            else:
                head.remove(todel)

        state['header'] = head 
        state['rows'] = [head]
    else:
        head = state['header']
        state['rows'].append([r[k] for k in head])



def delcol_flush(state, sep):
    outfile = re.sub('[^0-9]', '-', datetime.datetime.now().isoformat())
    outfile += '_' + state['csvfile']

    print('writing file %s ...' % outfile)

    with open(outfile, 'w') as f:
        writer = csv.writer(f, delimiter=sep)

        for row in state['rows']:
            writer.writerow(row)



def tojson(state, n, r, options):
    global header

    if 'header' not in state:
        head = list(header)
        state['header'] = head 
        state['rows'] = []
    else:
        head = state['header']
        state['rows'].append(r)



def tojson_flush(state, sep, options):
    outfile = re.sub('[^0-9]', '-', datetime.datetime.now().isoformat())
    outfile += '_' + state['csvfile'] + '.json'

    print('writing file %s ...' % outfile)
    compact = False

    op = options[0] if len(options) else None
    if op != None:
        if op == 'compact':
            compact = True
        else:
            print('WARNING: opzione "%s" sconosciuta' % op)

    with open(outfile, 'w') as f:
        if compact:
            json.dump(state['rows'], f)
        else:
            json.dump(state['rows'], f, sort_keys=True, indent=4, separators=(',', ': '))


def rawsplit(csvfile, options):
    nlines = 100
    if len(options):
        nlines = int(options[0])

    outfile1 = 'p1-' + csvfile
    outfile2 = 'p2-' + csvfile

    with open(csvfile) as f:
        head = f.readline()

        i = 0
        proc2 = False
        with open(outfile1, 'w') as f1:
            f1.write(head)
            while 1:
                line = f.readline()
                if not line:
                    break

                f1.write(line)
                if i > nlines:
                    proc2 = True
                    break

                i += 1

        print('scritte %s lines su %s' % (i, outfile1))

        i = 0
        with open(outfile2, 'w') as f2:
            f2.write(head)
            while 1:
                line = f.readline()
                if not line:
                    break

                f2.write(line)
                i += 1

        print('scritte %s lines su %s' % (i, outfile2))


def process(csvfile, mode, options):
    if mode == 'rawsplit':
        rawsplit(csvfile, options)
        return

    state = {'csvfile': csvfile}

    with open(csvfile) as f:
        sep = separator(f)
        for n, row in enumerate(csv.reader(f, delimiter=sep)):
            r = transcode(row, n)

            if not filterby(r) and n > 0:
                continue

            if mode == 'show':
                showline(n, r, options)
            elif mode == 'col':
                showcol(n, r, False, options)
            elif mode == 'coln':
                showcol(n, r, True, options)
            elif mode == 'check':
                showheader(n, r, options)
            elif mode == 'delcol':
                delcol(state, n, r, options)
            elif mode == 'csv':
                print(csvline(row, sep))
            elif mode == 'json':
                tojson(state, n, r, options)
            elif mode == 'splitby':
                splitby(state, n, r, sep, options) 
            else:
                print("unknow mode")
                usage()

    if mode == 'splitby':
        splitby_flush(state, sep)
    elif mode == 'delcol':
        delcol_flush(state, sep)
    elif mode == 'json':
        tojson_flush(state, sep, options)
    elif mode == 'check':
        print('linee totali', n)

def usage():
    usg = """
        usage:
            #PROG#  show     file.csv [ln]
            #PROG#  col      file.csv {column_name, ...}
            #PROG#  coln     file.csv {column_name, ...}
            #PROG#  csv      file.csv 
            #PROG#  rawsplit file.csv {nlines}
            #PROG#  splitby  file.csv {primarykey} [nline]
            #PROG#  delcol   file.csv {column_name}
            #PROG#  json     file.csv [compact] 
            #PROG#  check    file.csv  

        filtered (white and black list):
            WHITE="columnkey|value1|value2..." #PROG# file.csv csv
            BLACK="columnkey|value1|value2..." #PROG# file.csv csv
            FBLACK="columnkey|filename" #PROG# file.csv csv
            FWHITE="columnkey|filename" #PROG# file.csv csv
    """

    print(usg.replace('#PROG#', sys.argv[0].split('/')[-1]))
    sys.exit(0)

argv = sys.argv[1:]

if len(argv) < 2:
    usage()

mode = argv[0]
filename = argv[1]
options = argv[2:]

process(filename, mode, options)

