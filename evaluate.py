#!/usr/bin/env python

import argparse
import os
import shutil
import statistics

import matplotlib.pyplot as plt

def NullPlot(*args):
    pass

def PlotTransferMgr(filePath, plotOutputFilePath=None):
    plt.figure(num=filePath, figsize=(40.96, 21.60), dpi=100)
    plt.ticklabel_format(axis='both', style='plain')
    plt.grid(axis='y')
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
    plt.legend()
    plt.axis(xmin=0, ymin=0)
    if plotOutputFilePath:
        plt.savefig('{}.png'.format(plotOutputFilePath))
        plt.savefig('{}.svg'.format(plotOutputFilePath))

def PlotBilling(filePath, plotOutputFilePath=None):
    plt.figure(num=filePath, figsize=(40.96, 21.60), dpi=100)
    plt.ticklabel_format(axis='both', style='plain')
    plt.grid(axis='y')
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
    plt.legend()
    plt.axis(xmin=0, ymin=0)
    if plotOutputFilePath:
        plt.savefig('{}.png'.format(plotOutputFilePath))
        plt.savefig('{}.svg'.format(plotOutputFilePath))


parser = argparse.ArgumentParser(description='Evaluates sim output')

parser.add_argument('EvaluationNr', type=int, nargs='?', help='Running number of evaluations')
parser.add_argument('--input-base-path', dest='InputBasePath', default='')
parser.add_argument('--output-base-path', dest='OutputBasePath', default='results')
parser.add_argument('--force', dest='Force', action='store_true')

options = [
	{
		'FileOption': 'g2c-transfermgr-file',
		'DisableOption': 'no-g2c-transfermgr-plot',
		'FileDefault': 'g2c_transfers.dat',
		'DisableDefault': PlotTransferMgr
	},
	{
		'FileOption': 'c2c-transfermgr-file',
		'DisableOption': 'no-c2c-transfermgr-plot',
		'FileDefault': 'c2c_transfers.dat',
		'DisableDefault': PlotTransferMgr
	},
	{
		'FileOption': 'gcp-billing-file',
		'DisableOption': 'no-gcp-billing-plot',
		'FileDefault': 'GCP_storage.dat',
		'DisableDefault': PlotBilling
	},
	{
		'FileOption': 'gcp-network-file',
		'DisableOption': 'no-gcp-network-plot',
		'FileDefault': 'GCP_network.dat',
		'DisableDefault': NullPlot
	}
]

for option in options:
	parser.add_argument('--{}'.format(option['FileOption']), default=option['FileDefault'])
	parser.add_argument('--{}'.format(option['DisableOption']), default=option['DisableDefault'])

args = parser.parse_args()
argsDict = vars(args)


for option in options:
    srcFileName = argsDict.get(option['FileOption'].replace('-', '_'))
    srcFilePath = os.path.join(os.path.abspath(args.InputBasePath), srcFileName)
    if not os.path.isfile(srcFilePath):
        print('Could not find input file: {}'.format(srcFilePath))
        continue

    plotFilePath = None
    if args.EvaluationNr:
        dstDirPath = os.path.join(os.path.abspath(args.OutputBasePath), '{:0>4}'.format(int(args.EvaluationNr)))
        dstFilePath = os.path.join(dstDirPath, srcFileName)
        plotFilePath = os.path.join(dstDirPath, option['FileOption'])
        if not os.path.isdir(dstDirPath):
        	os.makedirs(dstDirPath)
        if not os.path.isfile(dstFilePath) or args.Force:
            shutil.copy2(srcFilePath, dstFilePath)
        else:
            print('Force disabled and file exists: {}'.format(dstFilePath))
    plotFunc = argsDict.get(option['DisableOption'].replace('-', '_'))
    plotFunc(srcFilePath, plotFilePath)

plt.show()
