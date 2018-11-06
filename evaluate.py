#!/usr/bin/env python

import os
import matplotlib.pyplot as plt
import statistics

bucketsById = {}

with open("GCP_storage.dat", "r") as infile:
    for line in infile:
        line = line.rstrip()
        if line == 'body':
            break
        items = line.split(':')
        bucketsById[items[0]] = {'name': items[1], 'x': [], 'y': []}
    data = infile.read().split('|')
    print('numEvents={}'.format(len(data)))
    for idx in range(0, len(data)-1, 3):
        bucket = bucketsById[data[i]]
        bucket['x'].append( int(data[i+1]) )
        bucket['y'].append( int(data[i+2]) )



for id in bucketsById:
    bucket = bucketsById[id]
    print('[{}] {}: ({}|{})'.format(id, bucket['name'], len(bucket['x']), len(bucket['y'])))
plt.figure(1)
for id in bucketsById:
    bucket = bucketsById[id]
    plt.plot(bucket['x'], bucket['y'], label=bucket['name'])
plt.legend()
plt.ylabel('volume GiB')
plt.xlabel('sim time/tick')

plt.show()
