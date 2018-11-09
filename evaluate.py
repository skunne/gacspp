#!/usr/bin/env python

import os
import matplotlib.pyplot as plt
import statistics

plt.ticklabel_format(axis='both', style='plain')
plt.grid(axis='y')

def PlotTransferMgr(filePath):
	with open(filePath, 'r') as infile:
		data = infile.read().split('|')
	items = (([],[]), ([],[]))
	for idx in range(0, len(data)-1, 3):
		eventId = int(data[idx])
		items[eventId][0].append(int(data[idx+1])/1000)
		items[eventId][1].append(int(data[idx+2]))
	plt.plot(items[0][0], items[0][1], label='NumActiveTransfers')
	plt.plot(items[1][0], items[1][1], label='NumTransfersToCreate')
	plt.ylabel('Count')
	plt.xlabel('Sim Time/1000')

def PlotBilling(filePath):
	bucketsById = {}
	with open(filePath, "r") as infile:
		for line in infile:
			line = line.rstrip()
			if line == 'body':
				break
			items = line.split(':')
			bucketsById[items[0]] = {'name': items[1], 'x': [], 'y': []}
		data = infile.read().split('|')
		print('numEvents={}'.format(len(data)))
		for idx in range(0, len(data)-10, 3):
			bucket = bucketsById[data[idx]]
			bucket['x'].append( int(data[idx+1])/1000 )
			bucket['y'].append( float(data[idx+2]) )

		for id in bucketsById:
			bucket = bucketsById[id]
			plt.plot(bucket['x'], bucket['y'], label=bucket['name'])

	plt.ylabel('Storage Volume [GiB]')
	plt.xlabel('Sim Time/1000')

plt.figure(1)
PlotTransferMgr("g2c_transfers.dat")
plt.figure(2)
PlotTransferMgr("c2c_transfers.dat")

plt.figure(3)
PlotBilling("GCP_storage.dat")
plt.axis(xmin=0, ymin=0)
plt.legend()

plt.show()
