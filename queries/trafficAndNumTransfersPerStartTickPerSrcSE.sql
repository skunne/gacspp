
select t.starttick, r.storageelementid, count(t.id), sum(r.filesize/(1024*1024))
from transfers t 
left join (select r.id as id, r.storageelementid as storageelementid, f.filesize as filesize
		   from replicas r, files f
		   where r.fileid = f.id) r
on t.srcreplicaid = r.id
group by r.storageelementid, t.starttick
order by t.starttick;