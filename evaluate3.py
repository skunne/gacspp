#!/usr/bin/env python

import argparse
import os
import shutil
import statistics

import psycopg2
import matplotlib.pyplot as plt

parser = argparse.ArgumentParser(description='Evaluates sim output')


conn = psycopg2.connect('dbname=gacspp user=postgres password=123')
db = conn.cursor()


q = "SELECT t.starttick, r.storageelementname, count(t.id), sum(r.filesize/(1024*1024)) "\
"FROM transfers t "\
"LEFT JOIN (SELECT r.id AS id, s.name AS storageelementname, f.filesize AS filesize "\
		   "FROM replicas r, files f, storageelements s "\
		   "WHERE r.fileid = f.id AND r.storageelementid = s.id) r "\
"ON t.srcreplicaid = r.id "\
"GROUP BY r.storageelementname, t.starttick "\
"ORDER BY r.storageelementname, t.starttick"


resW = 1920
resH = 1080
dpi = 100
plt.figure(num=1, figsize=(resW/dpi, resH/dpi), dpi=dpi)
plt.ticklabel_format(axis='both', style='plain')
plt.grid(axis='y')
print('query')
db.execute(q)
print('transforming')
t = db.fetchone()
print(t)
sid = t[1]
x = [t[0]]
y = [t[3]]
for t in db.fetchall():
	if t[1] != sid:
		plt.plot(x, y, label=sid)
		sid = t[1]
		x = [t[0]]
		y = [t[3]]
	else:
		x.append(t[0])
		y.append(y[-1] + t[3])
plt.plot(x, y, label=sid)
	
plt.xlabel('Sim Time')
plt.ylabel('Count')
plt.legend()
plt.axis(xmin=0, ymin=0)
print('saving')
plt.savefig('test.png')
print('plotting')
plt.show()

conn.close()
