--
-- DDL's DW Tool
--
--drop table dwmapr;
create table dwmapr (
 mapid      number(12,0), 
 mapref     varchar2(20), 
 mapdesc    varchar2(400), 
 trgschm    varchar2(30), 
 trgtbtyp   varchar2(10), 
 trgtbnm    varchar2(30), 
 frqcd      varchar2(3), 
 srcsystm   varchar2(30), 
 lgvrfyflg  varchar2(2), 
 lgvrfydt   date,
 stflg      varchar2(1), 
 reccrdt    date, 
 recupdt    date,
 curflg     varchar2(1),
 blkprcrows number(12),
 crtdby     varchar2(30),
 uptdby     varchar2(30),
 lgvrfby    varchar2(30),
 actby      varchar2(30),
 actdt      date );
 
alter table dwmapr add constraint dwmapr_pk primary key (mapid);
--drop sequence dwmaprseq;
create sequence dwmaprseq start with 1 increment by 1;
--
--drop table dwmaprdtl;
create table dwmaprdtl (
 mapdtlid  number(12,0), 
 mapref    varchar2(20), 
 trgclnm   varchar2(30), 
 trgcldtyp varchar2(30), 
 trgkeyflg  varchar2(1), 
 trgkeyseq  number(3), 
 trgcldesc varchar2(4000), 
 maplogic  varchar2(4000), 
 keyclnm   varchar2(250),
 valclnm   varchar2(30),
 mapcmbcd  varchar2(10), 
 excseq    number(10), 
 scdtyp    number(2), 
 lgvrfyflg varchar2(2), 
 lgvrfydt  date, 
 reccrdt   date,
 recupdt   date,
 curflg    varchar2(1),
 crtdby    varchar2(30),
 uptdby    varchar2(30)); 
 
alter table dwmaprdtl add constraint dwmarpdtl_pk primary key (mapdtlid);
--drop sequence dwmaprdtlseq;
create sequence dwmaprdtlseq start with 1 increment by 1;
--
--drop table dwjob;
create table dwjob (
 jobid   number(12),
 mapid    number(12),
 mapref   varchar2(20),
 frqcd    varchar2(3),
 trgschm  varchar2(30),
 trgtbtyp varchar2(10),
 trgtbnm  varchar2(30),
 srcsystm varchar2(30),
 stflg    varchar2(1),
 reccrdt  date,
 recupdt  date,
 curflg   varchar2(1),
 blkprcrows number(12) );
 
alter table dwjob add constraint dwjob_pk primary key (jobid);
--drop sequence dwjobseq;
create sequence dwjobseq start with 1 increment by 1;
--
--drop table dwjobdtl;
create table dwjobdtl(
 jobdtlid	number(12),
 mapref	    varchar2(20),
 mapdtlid   number(12),
 trgclnm    varchar2(30), 
 trgcldtyp  varchar2(30), 
 trgkeyflg   varchar2(1), 
 trgkeyseq   number(3), 
 trgcldesc  varchar2(4000), 
 trgnflg    varchar2(1), 
 maplogic	varchar2(4000),
 keyclnm    varchar2(250),
 valclnm    varchar2(30),
 mapcmbcd	varchar2(10),
 excseq	    number(10),
 scdtyp     number(2),
 reccrdt	date,
 recupdt    date,
 curflg	    varchar2(1));
--
alter table dwjobdtl add constraint dwjobdtl_pk primary key (mapref, jobdtlid);
--drop sequence dwjobdtlseq;
create sequence dwjobdtlseq start with 1 increment by 1;
--
--drop table dwjobflw;
create table dwjobflw(
jobflwid	number(12),
jobid	    number(12),
mapref	    varchar2(20),
trgschm	    varchar2(30),
trgtbtyp	varchar2(10),
trgtbnm	    varchar2(30),
dwlogic	    clob,
stflg	    varchar2(1),
recrdt	    date,
recupdt	    date,
curflg	    varchar2(1) );
--
alter table dwjobflw add constraint dwjobflow_pk primary key (jobflwid);
--drop sequence dwjobflwseq;
create sequence dwjobflwseq start with 1 increment by 1;
--

--drop table dwjobsch;
create table dwjobsch(
jobschid number(12),
jobflwid number(12),
mapref	 varchar2(20),
frqcd    varchar2(3),
frqdd	 varchar2(3),
frqhh	 varchar2(2),
frqmi	 varchar2(2),
strtdt   date,
enddt    date,
stflg	 varchar2(1),
curflg	 varchar2(1),
dpnd_jobschid number(12),
reccrdt	 date,
recupdt	 date,
schflg   varchar2(1)  );

alter table dwjobsch add constraint dwjobsch_pk primary key (jobschid);
--drop sequence dwjobschseq;
create sequence dwjobschseq start with 1 increment by 1;
--
--drop table dwjoblog;
create table dwjoblog (
joblogid number(12),
prcdt	 date,
jobid	 number(12),
mapref	 varchar2(20),
srcrows	 number(10),
trgrows	 number(10),
errrows  number(10),
reccrdt  date);

alter table dwjoblog add constraint dwjoblog_pk primary key (joblogid);
--drop sequence dwjoblogseq;
create sequence dwjoblogseq start with 1 increment by 1;
--
--drop table dwprclog;
create table dwprclog(
prcid	 number(12),
jobid	 number(12),
jobflwid number(12),
strtdt	 timestamp,
enddt	 timestamp,
status	 varchar2(2),
reccrdt	 timestamp,
recupdt	 date,
msg	     varchar2(400));

alter table dwprclog add (prclog varchar2(4000));

alter table dwprclog add constraint dwprclog_pk primary key (prcid);
--drop sequence dwprclogseq;
create sequence dwprclogseq start with 1 increment by 1;

--drop table dwjoberr;
create table dwjoberr(
joblogid number(12),
errid    number(12),
prcdt	 date,
jobid	 number(12),
mapref	 varchar2(20),
errtyp	 varchar2(10),
dberrmsg varchar2(4000),
errmsg   varchar2(400),
keyvalue varchar2(400),
reccrdt	 date);

alter table dwjoberr add constraint dwjoberr_pk primary key (joblogid, errid);
--drop sequence dwjoberrseq;
create sequence dwjoberrseq start with 1 increment by 1;
--
--drop table dwmaperr;
create table dwmaperr(
maperrid number(12),
mapdtlid number(12),
mapref   varchar2(20),
maplogic varchar2(4000),
errtyp	 varchar2(10),
errmsg	 varchar2(4000),
reccrdt	 date);

alter table dwmaperr add constraint dwmaperr_pk primary key (maperrid);
--drop sequence dwmaperrseq;
create sequence dwmaperrseq start with 1 increment by 1;
--

--drop table dwparams;
create table dwparams(
prtyp     varchar2(20),
prcd      varchar2(20),
prdesc    varchar2(200),
prval     varchar2(30),
prreccrdt date,
prrecupdt date);

alter table dwparams add constraint dwparams_pk primary key (prtyp, prcd);


-- Sample parameter data.
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'DB','DB','Database','Oracle',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'DB' and prcd = 'DB');
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'DB','Version','Identifies Database version.','19C',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'DB' and prcd = 'Version');
--
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'Datatype','String1','Identifies alpha-numeric value of 1 Character.','Varchar2(1)',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'String1');
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'Datatype','String3','Identifies alpha-numeric value of 3 Characters.','Varchar2(3)',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'String3');
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'Datatype','String5','Identifies alpha-numeric value of 5 Characters.','Varchar2(5)',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'String5');
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'Datatype','String10','Identifies alpha-numeric value of 10 Characters.','Varchar2(10)',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'String10');
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'Datatype','String20','Identifies alpha-numeric value of 20 Characters.','Varchar2(20)',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'String20');
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'Datatype','String30','Identifies alpha-numeric value of 30 Characters.','Varchar2(30)',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'String30');
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'Datatype','String50','Identifies alpha-numeric value of 50 Characters.','Varchar2(50)',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'String50');
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'Datatype','String100','Identifies alpha-numeric value of 100 Characters.','Varchar2(100)',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'String100');
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'Datatype','String250','Identifies alpha-numeric value of 250 Characters.','Varchar2(250)',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'String250');
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'Datatype','String4000','Identifies alpha-numeric value of 4000 Characters.','Varchar2(4000)',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'String4000');
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
--
select 'Datatype','Numeric1','Identifies numeric value of 1 digit.','Number(1)',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'Numeric1');
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'Datatype','Numeric3','Identifies numeric value of 3 digits.','Number(3)',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'Numeric3');
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'Datatype','Numeric5','Identifies numeric value of 5 digits.','Number(5)',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'Numeric5');
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'Datatype','Numeric10','Identifies numeric value of 10 digits.','Number(10)',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'Numeric10');
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'Datatype','Numeric20','Identifies numeric value of 20 digits.','Number(20)',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'Numeric20');
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'Datatype','Numeric30','Identifies numeric value of 30 digits.','Number(30)',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'Numeric30');
--
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'Datatype','Money10','Identfies currency value upto 10 digits','Number(10,6)',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'Money10');
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'Datatype','Money12','Identfies currency value upto 12 digits','Number(12,6)',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'Money12');
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'Datatype','Money18','Identfies currency value upto 18 digits','Number(18,6)',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'Money18');
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'Datatype','Money28','Identfies currency value upto 28 digits','Number(28,6)',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'Money28');
--
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'Datatype','Date','Identifies date values.','Date',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'Date');
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'Datatype','Timestamp','Identifies timestamp values.','Timestamp',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'Datatype' and prcd = 'Timestamp');
--
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'BULKPRC','NOOFROWS','Bulk Process Number of rows.','1000',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'BULKPRC' and prcd = 'NOOFROWS');
--
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'SCD','1','SCD Type 1 Implemented.','1',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'SCD' and prcd = '1');
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'SCD','2','Bulk Process Number of rows.','2',sysdate,sysdate
from dual 
where not exists(select null from dwparams where prtyp = 'SCD' and prcd = '2');

commit;

--
-- Change date: 30-May-2025
-- Mapping reference length increased to 50 Characters
--
alter table dwjobsch  modify mapref varchar2(50);
alter table dwjoblog  modify mapref varchar2(50);
alter table dwjoberr  modify mapref varchar2(50);
alter table dwmaperr  modify mapref varchar2(50);
alter table dwmapr    modify mapref varchar2(50);
alter table dwmaprdtl modify mapref varchar2(50);
alter table dwjob     modify mapref varchar2(50);
alter table dwjobdtl  modify mapref varchar2(50);
alter table dwjobflw  modify mapref varchar2(50);

-- 
-- Change date: 05-Jun-2025
-- Session and Process ID added to job log, process log and job error tables
--

alter table  dwjoblog 
add (prcid     number(12)
    ,sessionid number(30));
	
alter table  dwprclog
add(sessionid  number(30)
   ,mapref     varchar2(50)
   ,param1     varchar2(250)
   ,param2     varchar2(250)
   ,param3     varchar2(250)
   ,param4     varchar2(250)
   ,param5     varchar2(250)
   ,param6     varchar2(250)
   ,param7     varchar2(250)
   ,param8     varchar2(250)
   ,param9     varchar2(250)
   ,param10    varchar2(250) );
   
alter table  dwjoberr
add (prcid     number(12)
    ,sessionid number(30));

-- Change date: 26-Jun-2025
insert into dwparams (prtyp,prcd,prdesc,prval,prreccrdt,prrecupdt) 
select 'HIST_LOAD','DWT_PARAM1','DW parameter1.','01-Apr-2025',sysdate,sysdate
from dual
where not exists(select null from dwparams where prtyp = 'HIST_LOAD' and prcd = 'DWT_PARAM1');

-- Change Date: 28-Jun-2025

-- Table to capture SQL query.
-- drop table dwmaprsql;
create table dwmaprsql (
dwmaprsqlid  number(12),
dwmaprsqlcd  varchar2(100),
dwmaprsql    clob,
reccrdt	     date,
recupdt      date,
curflg	     varchar2(1) );

alter table dwmaprsql add constraint dwmaprsql_pk primary key (dwmaprsqlid);
--drop sequence dwmaprsqlseq;
create sequence dwmaprsqlseq start with 1 increment by 1;

alter table dwmaprdtl add (maprsqlcd  varchar2(100));
alter table dwjobdtl  add (maprsqlcd  varchar2(100));

alter table dwmapr    modify mapref varchar2(100);
alter table dwmaprdtl modify mapref varchar2(100);
alter table dwjob     modify mapref varchar2(100);
alter table dwjobdtl  modify mapref varchar2(100);
alter table dwjobflw  modify mapref varchar2(100);
alter table dwjobsch  modify mapref varchar2(100);
alter table dwjoblog  modify mapref varchar2(100);
alter table dwprclog  modify mapref varchar2(100);
alter table dwjoberr  modify mapref varchar2(100);
alter table dwmaperr  modify mapref varchar2(100);

alter table dwmapr    modify trgtbnm varchar2(100);
alter table dwjob     modify trgtbnm varchar2(100);

