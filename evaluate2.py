#!/usr/bin/env python

import argparse
import os
import shutil
import statistics

import sqlite3
import matplotlib.pyplot as plt

parser = argparse.ArgumentParser(description='Evaluates sim output')


conn = sqlite3.connect('16-05-43-output.db')
db = conn.cursor()


q = "SELECT bin_floor, bin_floor || ' to ' || bin_ceiling, count(*) "\
"FROM ("\
"   SELECT CAST( (endTick-startTick) / 100.0 as INT ) * 100 as bin_floor,"\
"          CAST( (endTick-startTick) / 100.0 as INT ) * 100 + 100 as bin_ceiling"\
"   FROM transfers) "\
"GROUP BY 1,2 "\
"ORDER BY 1"

bin_width = 1000
q = "SELECT bin_floor, bin_ceiling, count(*) "\
"FROM ("\
"   SELECT CAST( (endTick-startTick) / {0:f} as INT ) * {0:d} as bin_floor,"\
"          CAST( (endTick-startTick) / {0:f} as INT ) * {0:d} + {0:d} as bin_ceiling"\
"   FROM transfers) "\
"GROUP BY 1,2 "\
"ORDER BY 1".format(bin_width)

q = "SELECT bin_floor, count(*) "\
"FROM ("\
"   SELECT CAST( startTick / {0:f} as INT ) * {0:d} as bin_floor,"\
"          CAST( startTick / {0:f} as INT ) * {0:d} + {0:d} as bin_ceiling"\
"   FROM transfers) "\
"GROUP BY 1 "\
"ORDER BY 1".format(bin_width)

plt.figure(num=1, figsize=(25, 10), dpi=200)
plt.ticklabel_format(axis='both', style='plain')
plt.grid(axis='y')
data = db.execute(q)#"SELECT startTick, count(*) FROM transfers GROUP BY startTick;")
x, y = zip(*data)
plt.plot(x, y, label='NumStartedTransfers')
plt.xlabel('Sim Time')
plt.ylabel('Count')
plt.legend()
plt.axis(xmin=0, ymin=0)
plt.show()

conn.close()
