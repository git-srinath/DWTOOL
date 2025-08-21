--
-- Package for validating and processing mappings provided.
--
/*
Last error: 154
Change history:
date        who              Remarks
----------- ---------------- ------------------------------------------------------------------
28-May-2025 Srinath C V      CREATE_JOB_FLOW updated to use parameters.
30-May-2025 Srinath C V 	 CREATE_TARGET_TABLE updated to raise error is ddl is not generated.
05-Jun-2025 Srinath C V      CREATE_JOB_FLOW updated to handle parameter from DWPRCLOG table.
11-Jun-2025 Srinath C V      Amended CREATE_UPDATE_JOB to insert/update bulk rows values.
16-Jun-2025 Srinath C V      Amended CREATE_JOB_FLOW to handle multiple key columns correctly.
						     Amended GET_COLUMNS to truncate before assigning column values.
18-Jun-2025 Srinath C V      CREATE_JOB_FLOW amended to use PKGDWPRC.LOG_JOB_ERROR procedure to log errors.
24-Jun-2025 Srinath C V      Bug fix applied to CREATE_JOB_FLOW.
25-Jun-2025 Srinath C V      column order corrected in create target tables.
27-Jun-2025 Srinath C V      Amended CREATE_JOB_FLOW to handle null values.
28-Jun-2025 Srinath C V      Amended CREATE_JOB_FLOW to use SQL provided in the DWMAPRSQL table.
02-Jul-2025 Srinath C V      Amended CREATE_JOB_FLOW: gname variable size increased with in dynamic block.
10-Jul-2025 Srinath C V      Amended CREATE_TARGET_TABLE updated to create columns in execution sequence order.

*/
create or replace package body PKGDWJOB is 

  g_name constant varchar2(10) := 'PKGDWJOB';
  g_ver  constant varchar2(10) := 'V001';
   --
  type clnmtb_typ is table of dwjobdtl.trgclnm%type index by binary_integer;
  --
  function version return varchar is
  begin
    return g_name||':'||g_ver;
  end;
  
  --
  -- Private function to extract column names from string, delimited by comma.
  --

  function GET_COLUMNS(p_string in dwjobdtl.keyclnm%type)
  return   clnmtb_typ is
  /*
   Change history:
   date        who              Remarks
   ----------- ---------------- ------------------------------------------------------------------
   16-Jun-2025 Srinath C V      Amended GET_COLUMNS to truncate before assigning column values.

*/
     --
     w_procnm  varchar2(50) := 'GET_COLUMNS';
     w_parm    varchar2(400) := substr(p_string,1,400);
     --
     w_tbl     clnmtb_typ;
     w_idx     integer := 0;
     w_idx2    integer := 0;
     w_string  dwjobdtl.keyclnm%type := rtrim(trim(p_string),',');
  begin
    if w_string is not null then
       loop
         w_idx := w_idx + 1;
         w_idx2 := instr(w_string,',',1);
         --
         if w_idx2 > 0 then
            w_tbl(w_idx) := trim(substr(w_string, 1, w_idx2 -1));
         else
            w_tbl(w_idx) := trim(w_string);
         end if;
         --
         w_string := substr(w_string,w_idx2 + 1);
         --
         if w_idx2 = 0 then
           exit;
         end if;
         --
       end loop;
    end if;
    --
    return w_tbl;
    --
  exception
    when others then
      PKGERR.RAISE_ERROR(g_name, w_procnm,'001', w_parm);
  end GET_COLUMNS;
   
  --
  -- Function to create target tables as per mapping.
  --
  function CREATE_TARGET_TABLE(p_mapref in dwmapr.mapref%type)
  return   varchar2 is
    --
    w_procnm  varchar2(50) := 'CREATE_TARGET_TABLE';
    --
    w_parm    varchar2(200) := substr(p_mapref,1,200);
    --
	cursor jobdtl_cur is
    select jd.mapref, j.trgschm, j.trgtbtyp, j.trgtbnm, u.table_name
	      ,jd.trgclnm, jd.trgcldtyp, jd.trgkeyflg, jd.trgkeyseq
          ,p.prval, c.column_name
    from  dwjob j,dwjobdtl jd, dwparams p, user_tables u, user_tab_columns c
    where j.mapref = p_mapref
	and   j.curflg = 'Y'
	and   jd.mapref = j.mapref
    and   jd.curflg = 'Y'
    and   p.prtyp = 'Datatype'
    and   p.prcd = jd.trgcldtyp
	and   u.table_name (+) = j.trgtbnm
	and   c.table_name (+) = u.table_name
	and   c.column_name(+) = jd.trgclnm
	order by jd.excseq;
    --
	cursor seq_csr (c_seq user_sequences.sequence_name%type) is
	select sequence_name
	from   user_sequences
	where  sequence_name = c_seq;
	--
	w_seq_rec seq_csr%rowtype     := null;
	--
    w_tbtyp   dwjob.trgtbtyp%type := null;
	w_trgtbnm dwjob.trgtbnm%type  := null;
    --
	w_flg     varchar2(1)         := 'N';
	w_tbnm    varchar2(70)        := null;
	--                            
    w_ddl     varchar2(32767)     := null;
    w_seqddl  varchar2(4000)      := null;
    --                            
    w_cursor  number(12)          := null;
    w_output  number(12)          := null;
    --                            
    w_return  varchar2(1)         := 'Y';
	w_cnt     integer             := 0;
	w_msg     varchar2(200)       := null;
	--                               
  begin
    for jd_rec in jobdtl_cur loop
	  w_cnt := w_cnt + 1;
	  --
	  if w_cnt = 1 then
	     w_trgtbnm := jd_rec.trgtbnm;
	     w_tbnm    := jd_rec.trgschm||'.'||jd_rec.trgtbnm;
		 w_tbtyp   := jd_rec.trgtbtyp;
		 --
	     if jd_rec.table_name is not null then
		    w_flg := 'Y';
		 end if;
		 --
	  end if;
      --
	  if jd_rec.column_name is null then
	     w_ddl := w_ddl ||jd_rec.trgclnm||' '||jd_rec.prval||','||chr(10);
	  end if;
      --
    end loop;
    --
	if w_ddl is null and w_flg = 'N' then
	   w_msg := 'Target table not created (failed to generate ddl), please verify mappings.';
	   w_parm := w_parm || '::' ||w_msg;
	   raise value_error;
	end if;
	--
    if w_flg = 'N' then
       -- New table to be created.
       if w_ddl is not null then
          w_ddl := 'create table '|| w_tbnm ||' ('||chr(10)||
		           case when w_tbtyp in ('DIM','FCT','MRT') then
                     'SKEY  number(20) primary key,'||chr(10)
				   else 
				     ''
				   end || w_ddl ||
				   case when w_tbtyp = 'DIM' then
                     'CURFLG VARCHAR2(1),'||chr(10)||
                     'FROMDT DATE,'||chr(10)||
                     'TODT   DATE,'||chr(10)
				   else 
				     ''
				   end ||
				   'RECCRDT  DATE,'||chr(10)||
                   'RECUPDT  DATE )';
	   end if;
	   --
	else
	   if w_ddl is not null then
	      w_ddl := 'alter table '||w_tbnm || ' add ('||chr(10)||
		           rtrim(w_ddl,','||chr(10))||')';
	   end if;
    end if;	
	--
	if w_tbtyp in ('DIM','FCT','MRT') then
	   declare
	     w_pm  varchar2(100) := substr(w_tbnm||'_SEQ',1,100);
	     w_seq varchar2(60)  := w_trgtbnm||'_SEQ';
	   begin
	     open seq_csr(w_seq);
	 	 fetch seq_csr into w_seq_rec;
	     close seq_csr;
	     --
	     if w_seq_rec.sequence_name is null then
	        w_seqddl := 'create sequence '||w_seq||' start with 1 increment by 1';
	     end if;
	   exception
	     when others then
	       PKGERR.RAISE_ERROR(g_name, w_procnm,'101', w_pm);
	   end;
	end if;
    --
    -- dbms_output.put_line('w_ddl = '||w_ddl);
	--
    w_cursor := DBMS_SQL.OPEN_CURSOR;
    --
	if w_ddl is not null then
       begin
         DBMS_SQL.PARSE(w_cursor, w_ddl, dbms_sql.native);
       exception
         when others then
          w_return := 'N';
          PKGERR.RAISE_ERROR(g_name, w_procnm,'102', w_parm);
       end;
       --
       if w_return = 'Y' then
          w_output := DBMS_SQL.EXECUTE(w_cursor);
       end if;
	end if;
    --
	--dbms_output.put_line('w_seqddl = '||w_seqddl);
	--
    if w_seqddl is not null then
       begin
         DBMS_SQL.PARSE(w_cursor, w_seqddl, dbms_sql.native);
       exception
         when others then
          w_return := 'N';
          PKGERR.RAISE_ERROR(g_name, w_procnm,'103', w_parm);
       end;
       --
       if w_return = 'Y' then
          w_output := DBMS_SQL.EXECUTE(w_cursor);
       end if;
       --
    end if;
	--
    DBMS_SQL.CLOSE_CURSOR(w_cursor);
    --
	return w_return;
    --
  exception
    when others then 
     PKGERR.RAISE_ERROR(g_name, w_procnm,'104', w_parm);
  end CREATE_TARGET_TABLE;
  --
  -- Function to create jobs basis mappings.
  --
  function CREATE_UPDATE_JOB(p_mapref in dwmapr.mapref%type) 
  return   dwjob.jobid%type is
  /*
   Change history:
   date        who              Remarks
   ----------- ---------------- ------------------------------------------------------------------
   11-Jun-2025 Srinath C V      Amended CREATE_UPDATE_JOB to insert/update bulk rows values.
   28-Jun-2025 Srinath C V      Amended to populate MAPRSQLCD in DWJOBDTL table.

  */
    --
    w_procnm  varchar2(50) := 'CREATE_UPDATE_JOB';
    --
    w_parm    varchar2(200) := substr(p_mapref,1,200);
    --
    cursor mapr_cur (c_mapref dwmapr.mapref%type) is 
    select * 
    from  dwmapr 
    where mapref    = c_mapref
    and   curflg    = 'Y'
    and   lgvrfyflg = 'Y'
    and   stflg     = 'A';
    --
    w_jobid  dwjob.jobid%type := null;
    --
  begin
    --
    for map_rec in mapr_cur(p_mapref) loop
      --
      declare
        w_pm  varchar2(400) := substr('Mapref='||map_rec.mapref||
                              ' Trgtb='||map_rec.trgtbnm,1,400);
          --
        cursor job_cur is
        select *
        from   dwjob
        where  mapref = map_rec.mapref
        and    curflg = 'Y'
        and    stflg = 'A';
        --
        w_job_rec job_cur%rowtype;
        --
        w_chg     varchar2(1) := 'Y';
      begin
        open job_cur;
        fetch job_cur into w_job_rec;
        close job_cur;
        --
        if w_job_rec.jobid is not null then
           if w_job_rec.frqcd      != map_rec.frqcd
           or w_job_rec.stflg      != map_rec.stflg
           or nvl(w_job_rec.blkprcrows,-1) != map_rec.blkprcrows then
              w_chg := 'Y';
              --
              declare
                w_pm  varchar2(100) := substr('Jobid='||w_job_rec.jobid,1,100);
              begin
                update dwjob
                set    curflg = 'N'
                      ,recupdt = sysdate
                where jobid = w_job_rec.jobid
                and   curflg = 'Y';
              exception
                 when others then
                   PKGERR.RAISE_ERROR(g_name, w_procnm,'105', w_pm);
              end;
           else
              w_chg := 'N';
           end if;
        end if;          
        --
        if w_chg = 'Y' then 
           insert into dwjob(jobid, mapid, mapref, frqcd, trgschm, trgtbtyp
                            ,trgtbnm, srcsystm, stflg, reccrdt, recupdt, curflg, blkprcrows)
             values (dwjobseq.nextval, map_rec.mapid, map_rec.mapref, map_rec.frqcd
                  ,map_rec.trgschm, map_rec.trgtbtyp, map_rec.trgtbnm, map_rec.srcsystm
                   ,map_rec.stflg, sysdate, sysdate, 'Y', map_rec.blkprcrows)
           returning jobid into w_jobid;
        else
          w_jobid := w_job_rec.jobid;
        end if;
      exception
        when others then
          PKGERR.RAISE_ERROR(g_name, w_procnm,'106', w_pm);
      end;
      --
      if w_jobid is not null then
         declare
           w_pm  varchar2(100) := substr('Mapref='||map_rec.mapref,1,100);
           --
           cursor mapdtl_cur is
           select *
           from   dwmaprdtl
           where  mapref = map_rec.mapref
           and    curflg = 'Y';
           --
         begin
           for mapdtl_rec in mapdtl_cur loop
             -- dbms_output.put_line('mapdtl_rec.trgclnm='||mapdtl_rec.trgclnm);
             --
             declare
               w_pm  varchar2(400) := substr('Mapref='||mapdtl_rec.mapref||
                                             ' Jobid='||w_jobid||
                                             ' Mapdtlid='||mapdtl_rec.mapdtlid||
                                             ' Trgtb='||map_rec.trgtbnm||
                                             ' Trgcl='||mapdtl_rec.trgclnm,1,400);
               --
               cursor  jobdtl_cur (c_mapref  dwjobdtl.mapref%type
                                  ,c_trgclnm dwjobdtl.trgclnm%type) is 
               select * 
               from  dwjobdtl 
               where mapref  = c_mapref
               and   trgclnm = c_trgclnm
               and   curflg  = 'Y';
               --
               w_jobdtl_rec  jobdtl_cur%rowtype;
               --
               w_chg         varchar2(1) := 'Y';
             begin
               --
               open  jobdtl_cur(mapdtl_rec.mapref, mapdtl_rec.trgclnm);
               fetch jobdtl_cur into w_jobdtl_rec;
               close jobdtl_cur;
               --
               if w_jobdtl_rec.mapref is not null then
                  if w_jobdtl_rec.trgcldesc         != mapdtl_rec.trgcldesc
                  or w_jobdtl_rec.maplogic          != mapdtl_rec.maplogic 
                  or w_jobdtl_rec.keyclnm           != mapdtl_rec.keyclnm  
                  or w_jobdtl_rec.valclnm           != mapdtl_rec.valclnm  
                  or w_jobdtl_rec.mapcmbcd          != mapdtl_rec.mapcmbcd 
				  or w_jobdtl_rec.maprsqlcd         != mapdtl_rec.maprsqlcd
                  or w_jobdtl_rec.excseq            != mapdtl_rec.excseq  
				  or nvl(w_jobdtl_rec.trgkeyseq,-1) != nvl(mapdtl_rec.trgkeyseq,-1)
                  or nvl(w_jobdtl_rec.scdtyp,1)     != nvl(mapdtl_rec.scdtyp,1) then
                     w_chg := 'Y';
                     --
                     declare
                       w_pm  varchar2(100) := substr('Mapref='||w_jobdtl_rec.mapref||
                                                    ' JobDtlId='||w_jobdtl_rec.jobdtlid,1,100);
                     begin
                       update dwjobdtl
                       set    curflg = 'N'
                             ,recupdt = sysdate
                       where mapref = w_jobdtl_rec.mapref
                       and   jobdtlid = w_jobdtl_rec.jobdtlid
                       and   curflg = 'Y';
                     exception
                       when others then
                         PKGERR.RAISE_ERROR(g_name, w_procnm,'107', w_pm);
                     end;
                  else
                     w_chg := 'N';
                  end if;
                  --
               end if;
               --
               if w_chg = 'Y' then              
                insert into 
                dwjobdtl(jobdtlid, mapref, mapdtlid, trgclnm
				        ,trgcldtyp, trgkeyflg, trgkeyseq, trgcldesc
                        ,maplogic, maprsqlcd, keyclnm, valclnm, mapcmbcd
                        ,excseq, scdtyp, reccrdt, recupdt, curflg)
                values (dwjobdtlseq.nextval, mapdtl_rec.mapref, mapdtl_rec.mapdtlid, mapdtl_rec.trgclnm
				       ,mapdtl_rec.trgcldtyp, mapdtl_rec.trgkeyflg, mapdtl_rec.trgkeyseq, mapdtl_rec.trgcldesc
                       ,mapdtl_rec.maplogic, mapdtl_rec.maprsqlcd, mapdtl_rec.keyclnm, mapdtl_rec.valclnm, mapdtl_rec.mapcmbcd
                       ,mapdtl_rec.excseq, mapdtl_rec.scdtyp, sysdate, sysdate, 'Y');
               end if;
               --
             exception
               when others then
                 PKGERR.RAISE_ERROR(g_name, w_procnm,'108', w_pm);
             end;
           end loop;
         exception
           when others then
             PKGERR.RAISE_ERROR(g_name, w_procnm,'109', w_pm);
         end;
         --      
      end if;
      --
    end loop;
	--
	commit;
	--
    --
	-- Create the target table.
	--
    declare
      w_stat varchar2(1) := null;
    begin
      w_stat := CREATE_TARGET_TABLE(p_mapref);
	  --
	  if nvl(w_stat,'Y') = 'Y' then
	     --
	     -- Create job flow.
	     --
         begin
           PKGDWJOB.CREATE_JOB_FLOW(p_mapref);
	     exception
           when others then
             PKGERR.RAISE_ERROR(g_name, w_procnm,'110', w_parm);
         end;
	  end if;
    exception
      when others then
       PKGERR.RAISE_ERROR(g_name, w_procnm,'111', w_parm);
    end;
	--
    return w_jobid;
    --
  exception
    when others then 
     PKGERR.RAISE_ERROR(g_name, w_procnm,'112', w_parm);
  end CREATE_UPDATE_JOB;
   
  --
  -- Procedure to create jobs for all the mappings.
  --
  Procedure CREATE_ALL_JOBS is
    --
    w_procnm  varchar2(50) := 'CREATE_UPDATE_JOB';
    --
    w_parm    varchar2(200) := null;
    --
    cursor map_cur is 
    select mapref 
    from  dwmapr 
    where curflg    = 'Y'
    and   lgvrfyflg = 'Y'
    and   stflg     = 'A';
  begin
    for map_rec in map_cur loop
      declare
       w_pm    varchar2(200)    := map_rec.mapref;
       w_jobid dwjob.jobid%type := null;
     begin
       w_jobid := CREATE_UPDATE_JOB(map_rec.mapref);
     exception
       when others then
         PKGERR.RAISE_ERROR(g_name, w_procnm,'113', w_pm);
      end;
    end loop;
  
  exception
    when others then 
     PKGERR.RAISE_ERROR(g_name, w_procnm,'114', w_parm);
  end CREATE_ALL_JOBS;

  --
  -- Procedure to process a job basis mapping reference.
  --
  procedure CREATE_JOB_FLOW (p_mapref  in  dwmapr.mapref%type) is
  /*
  Change history:
  date        who              Remarks
  ----------- ---------------- ------------------------------------------------------------------
  28-May-2025 Srinath C V      updated to use DWT parameters.
  05-Jun-2025 Srinath C V      updated to handle parameter from DWPRCLOG table.
							   Capture sessionid and PRCID in DWJOBLOG table.
  16-Jun-2025 Srinath C V      Amended CREATE_JOB_FLOW to handle multiple key columns correctly.
  18-Jun-2025 Srinath C V      CREATE_JOB_FLOW amended to use PKGDWPRC.LOG_JOB_ERROR procedure to log errors.
  24-Jun-2025 Srinath C V      bug fix: Main cursor never closed.
  27-Jun-2025 Srinath C V      Amended to handle null values.
  28-Jun-2025 Srinath C V      Amended CREATE_JOB_FLOW to use SQL provided in the DWMAPRSQL table.
  02-Jul-2025 Srinath C V      Amended CREATE_JOB_FLOW: gname variable size increased with in dynamic block.

  */
    --
    w_procnm  varchar2(50) := 'CREATE_JOB_FLOW';
    --
    w_parm    varchar2(200) := substr(p_mapref,1,200);
    --
    cursor jobcsr(c_mapref dwjob.mapref%type) is
    select jobid, mapref, trgschm, trgtbnm, trgtbtyp, trgschm||'.'||trgtbnm tbnam, blkprcrows
    from   dwjob
    where  mapref = c_mapref
    and    stflg  = 'A'
    and    curflg = 'Y';
    --
    w_job_rec jobcsr%rowtype := null;
    --
    cursor jdpk_cur(c_mapref dwjob.mapref%type) is
    select jd.mapref, jd.trgclnm, jd.trgcldtyp, jd.trgkeyflg, jd.trgkeyseq
    from   dwjobdtl jd
    where jd.mapref = c_mapref
    and   jd.curflg = 'Y'
    and   jd.trgkeyflg = 'Y'
    order by jd.trgkeyseq;
    --
    cursor trg_cur is
    select j.jobid, j.mapref, j.trgschm, j.trgtbnm, j.trgtbtyp, jd.trgclnm
    from   dwjob j, dwjobdtl jd
    where j.mapref = p_mapref
    and   j.stflg = 'A'
    and   j.curflg = 'Y'
    and   jd.mapref = j.mapref
    and   jd.curflg = 'Y';
    --
    cursor cmb_cur (c_mapref dwjobdtl.mapref%type) is
    select jd.mapcmbcd, min(nvl(jd.trgkeyseq,9999)) kseq, nvl(jd.scdtyp,1) scdtyp, max(jd.excseq) maxexcseq
    from   dwjobdtl jd
    where jd.mapref = c_mapref
    and   jd.curflg = 'Y'
    and   jd.mapcmbcd is not null
    group by jd.mapcmbcd, nvl(jd.scdtyp,1)
    order by min(nvl2(jd.trgkeyseq,1,2)), max(jd.excseq), nvl(jd.scdtyp,1) desc;
     --
    cursor jd_cur (c_jobid     dwjob.jobid%type
                  ,c_mapcmbcd  dwjobdtl.mapcmbcd%type
                  ,c_scdtyp    dwjobdtl.scdtyp%type) is
    select j.mapref, j.trgschm, j.trgtbtyp, j.trgtbnm
          ,jd.trgclnm, jd.trgcldtyp, jd.trgnflg, jd.maplogic, jd.trgkeyflg
          ,jd.keyclnm, jd.valclnm, jd.mapcmbcd, jd.excseq, p.prval
		  ,jd.maprsqlcd, s.dwmaprsql
    from  dwjob j, dwjobdtl jd, dwmaprsql s, dwparams p
    where j.jobid = c_jobid
    and   j.stflg = 'A'
    and   j.curflg = 'Y'
    and   jd.mapref = j.mapref
    and   jd.curflg = 'Y'
    and   nvl(jd.scdtyp,1) = nvl(c_scdtyp,1)
    and   nvl(jd.mapcmbcd,'#') = nvl(c_mapcmbcd,'#')
	and   s.dwmaprsqlcd(+) = jd.maprsqlcd
	and   s.curflg(+) = 'Y'
    and   p.prtyp = 'Datatype'
    and   p.prcd  = jd.trgcldtyp
    order by nvl2(jd.trgkeyseq,1,2), jd.excseq;
    --
    w_limit   number(10)      := 0;
    w_lnbrk   varchar2(1)     := chr(10);
    w_cmbcnt  number(10)      := 0;
	w_errnbr  number(5)       := 900;
	w_fstcnt  number(10)      := null;
    --
    w_pktb    clnmtb_typ;
    w_dtins1  clob;
    w_dtins2  clob;
    w_scd1upd clob;
    w_errstr  varchar2(400)   := '';
    --
    w_text    varchar2(32767) := '';
	--
	w_maprsqlcd dwjobdtl.maprsqlcd%type  := null;
	w_maprsql   dwmaprsql.dwmaprsql%type := null;
	--
    w_plsql   clob;
    w_plsql1  clob;
    w_plsql2  clob;
    w_plsql3  clob;
    --
    w_csr1key clnmtb_typ;
    --
  begin
    --
    -- Below block gets the parameter value set for number rows to process in a batch.
    --
    begin
      select prval into w_limit
      from   dwparams
      where  prtyp = 'BULKPRC'
      and    prcd = 'NOOFROWS';
    exception
      when others then
       w_limit := 1000;
    end;
    --
    begin
	  DBMS_LOB.CREATETEMPORARY(w_dtins1,  True,2);
      DBMS_LOB.CREATETEMPORARY(w_dtins2,  True,2);
      DBMS_LOB.CREATETEMPORARY(w_scd1upd, True,2);
      DBMS_LOB.CREATETEMPORARY(w_plsql,   True,2);
      DBMS_LOB.CREATETEMPORARY(w_plsql1,  True,2);
      DBMS_LOB.CREATETEMPORARY(w_plsql2,  True,2);
      DBMS_LOB.CREATETEMPORARY(w_plsql3,  True,2);
    exception
      when others then
        PKGERR.RAISE_ERROR(g_name, w_procnm,'117', w_parm);
    end;
    --
    -- Below block is to get target table and its detials.
    --
    begin
      open jobcsr(p_mapref);
      fetch jobcsr into w_job_rec;
      close jobcsr;
    exception
      when others then 
        PKGERR.RAISE_ERROR(g_name, w_procnm,'115', w_parm);
    end;
    --
    Declare
      w_idx integer := 0;
      i     integer := 0;
    begin
      --
      -- Below block is build require where clause with key columns of target table.
      --
      for jdpk_rec in jdpk_cur(p_mapref) loop
        w_idx := w_idx+1;
        w_pktb(w_idx) := jdpk_rec.trgclnm;
        w_errstr := w_errstr||'w_trgtb1(e).'||jdpk_rec.trgclnm||'||';
      end loop;
      --
      -- Below block is ensure columns string is in correct order.
      --
      for trg_rec in trg_cur loop
	    DBMS_LOB.WRITEAPPEND(w_dtins1,length(trg_rec.trgclnm||','),trg_rec.trgclnm||',');
	    DBMS_LOB.WRITEAPPEND(w_dtins2,length('w_trgtb1(i).'||trg_rec.trgclnm||','),'w_trgtb1(i).'||trg_rec.trgclnm||',');
      end loop;
    exception
       when others then 
         PKGERR.RAISE_ERROR(g_name, w_procnm,'116', w_parm);
    end;
    --
    for cmb_rec in cmb_cur (w_job_rec.mapref) loop
      --
      w_cmbcnt := w_cmbcnt + 1;
      --
	  --dbms_output.put_line('cmbcnt = '||w_cmbcnt);
	  --
	  declare
	    w_len integer;
	  begin
	    DBMS_LOB.CREATETEMPORARY(w_maprsql,True,2);
	    w_maprsqlcd := null;
	  exception
        when others then 
          PKGERR.RAISE_ERROR(g_name, w_procnm,'146', w_parm);
      end;
      --
      if w_cmbcnt = 1 then
         begin
           w_text := 'Declare'||w_lnbrk||
		             ' dwt_param1  varchar2(250) := null;'||w_lnbrk||
					 ' dwt_param2  varchar2(250) := null;'||w_lnbrk||
					 ' dwt_param3  varchar2(250) := null;'||w_lnbrk||
					 ' dwt_param4  varchar2(250) := null;'||w_lnbrk||
					 ' dwt_param5  varchar2(250) := null;'||w_lnbrk||
					 ' dwt_param6  varchar2(250) := null;'||w_lnbrk||
					 ' dwt_param7  varchar2(250) := null;'||w_lnbrk||
					 ' dwt_param8  varchar2(250) := null;'||w_lnbrk||
					 ' dwt_param9  varchar2(250) := null;'||w_lnbrk||
					 ' dwt_param10 varchar2(250) := null;'||w_lnbrk||
					 ' --'||w_lnbrk||
                     ' g_name   varchar2(120) := ''DYNBLK-'||w_job_rec.mapref||''';'||w_lnbrk||
                     ' w_procnm varchar2(120) := '''||w_job_rec.mapref||''';'||w_lnbrk||
                     ' w_parm   varchar2(400):= ''CombCode='||cmb_rec.mapcmbcd||''';'||w_lnbrk||
					 ' w_prcid  dwprclog.prcid%type  := null;'||w_lnbrk||
					 ' w_sessionid dwprclog.sessionid%type := null;'||w_lnbrk||
                     ' i        integer      := 0;'||w_lnbrk||
					 ' i1       integer      := 0;'||w_lnbrk||
					 ' i1_cnt   integer      := 0;'||w_lnbrk||
					 ' i2       integer      := 0;'||w_lnbrk||
					 ' i2_cnt   integer      := 0;'||w_lnbrk||
					 ' i3       integer      := 0;'||w_lnbrk||
					 ' i3_cnt   integer      := 0;'||w_lnbrk||
                     ' w_srccnt number(20)   := 0;'||w_lnbrk||
                     ' w_trgcnt number(20)   := 0;'||w_lnbrk||
                     ' w_errcnt number(20)   := 0;'||w_lnbrk||
                     ' w_sysdt  date         := trunc(sysdate,''MI'');'||w_lnbrk||
                     ' w_hidt   date         := to_date(''29991231'',''YYYYMMDD'');'||w_lnbrk||w_lnbrk||
                     ' blk_err  exception;'||w_lnbrk||
                     ' pragma exception_init(blk_err, -24381);'||w_lnbrk||w_lnbrk||
                     ' w_joblogid dwjoblog.joblogid%type := null;'||w_lnbrk||w_lnbrk||
                     ' type trgtbtyp is table of '||w_job_rec.tbnam||'%rowtype index by binary_integer;'||w_lnbrk||
                     ' type keytbtyp is table of '||w_job_rec.tbnam||'.skey%type index by binary_integer;'||w_lnbrk||
                     ' w_trgrec  '||w_job_rec.tbnam||'%rowtype;'||w_lnbrk||
                     ' w_trgtb1  trgtbtyp;'||w_lnbrk||
                     ' w_trgtb2  trgtbtyp;'||w_lnbrk||
                     ' w_trgtb3  keytbtyp;'||w_lnbrk||w_lnbrk;
           DBMS_LOB.WRITEAPPEND(w_plsql1, length(w_text), w_text);
         exception
           when others then
             PKGERR.RAISE_ERROR(g_name, w_procnm,'118', w_parm);
         end;
      end if;
      --
      declare
        w_cnt     integer          := 0;
        --
        w_cursor  varchar2(100)    := 'Cursor csr'||w_cmbcnt||' is';		
        w_with    varchar2(32767)  := w_cursor||w_lnbrk||' with ';
        w_sel     varchar2(32767)  := 'select ';
        w_frm     varchar2(32767)  := 'from '; 
		--
        w_whr     clob;
        w_asgn1   clob; -- SCD2 update string
        w_asgn2   clob; -- SCD1 update string
        w_dtwhr   clob;
        --                         
        w_whr2    varchar2(32767)  := '';
        w_pkstr   varchar2(32767)  := '';
        w_str1    varchar2(20)     := '';
        --
        w_clnms1  clnmtb_typ;
        w_clnms2  clnmtb_typ;
      begin
        --
		DBMS_LOB.CREATETEMPORARY(w_whr,   True, 2);
		DBMS_LOB.CREATETEMPORARY(w_dtwhr, True, 2);
		DBMS_LOB.CREATETEMPORARY(w_asgn1, True, 2);
		DBMS_LOB.CREATETEMPORARY(w_asgn2, True, 2);
		--
		DBMS_LOB.WRITEAPPEND(w_whr, length('where 1 = 1'||w_lnbrk), 'where 1 = 1'||w_lnbrk);
		--
        for jd_rec in jd_cur(w_job_rec.jobid, cmb_rec.mapcmbcd, cmb_rec.scdtyp) loop
          declare 
            w_str     varchar2(20)          := 'sql';
            i         integer               := 0;
          begin
            w_cnt := w_cnt + 1;
            --
            w_str := w_str||to_char(w_cnt);
            --
            if w_cmbcnt > 1 and w_cnt > 1then
               w_with := w_with||'     ';
            end if;
            --
            w_with := w_with||' '||w_str||' as ('||jd_rec.maplogic||')'||','||w_lnbrk;
            --
			--dbms_output.put_line('JD_REC: w_cnt = '||w_cnt);
			--
            case w_cnt
            when 1 then
              --
              declare
                w_pm varchar2(400) := jd_rec.keyclnm;
              begin
                w_clnms1 := GET_COLUMNS(jd_rec.keyclnm);
                --
				--dbms_output.put_line('w_clnms1(1) = '||w_clnms1(1));
				--
                if w_cmbcnt = 1 then
                   w_csr1key := w_clnms1;
                end if;
                --    
                w_str1 := w_str;
              exception
                when others then 
                  PKGERR.RAISE_ERROR(g_name, w_procnm,'119', w_pm);
              end;
              --
			  declare
                w_pm varchar2(400) := jd_rec.keyclnm;
              begin
                for i in 1..w_pktb.count loop 
                  w_pkstr  := w_pkstr||'and dtb.'||w_pktb(i)||' = w_csr1_tab(i).'||w_clnms1(i)||w_lnbrk;
                end loop;
			  exception
                when others then 
                  PKGERR.RAISE_ERROR(g_name, w_procnm,'142', w_pm);
              end;
              --
			  declare
                w_pm varchar2(400) := jd_rec.keyclnm;
              begin
                for i in 1..w_clnms1.count loop
				  declare
				    w_txt1 varchar2(4000);
					w_txt2 varchar2(4000);
				  begin
                    w_txt1 := '            ';
                    w_txt2 := '            ';
                    --
				    w_sel  := w_sel||w_str||'.'||w_clnms1(i)||',';
				    --
                    w_txt1 := w_txt1||'w_trgtb1(i1).'||w_pktb(i)||' := w_csr'||w_cmbcnt||
                               case when w_cmbcnt = 1 then '_tab(i).' else '_rec.' end ||w_clnms1(i)||';'||w_lnbrk;
                    w_txt2 := w_txt2||'w_trgtb2(i2).'||w_pktb(i)||' := w_csr'||w_cmbcnt||
                               case when w_cmbcnt = 1 then '_tab(i).' else '_rec.' end ||w_clnms1(i)||';'||w_lnbrk;
                    --
					DBMS_LOB.WRITEAPPEND(w_asgn1, length(w_txt1), w_txt1);
					DBMS_LOB.WRITEAPPEND(w_asgn2, length(w_txt2), w_txt2);
				  exception
				    when others then
					  PKGERR.RAISE_ERROR(g_name, w_procnm,'147', w_pm);
				  end;
                end loop;
				--
				w_sel := rtrim(w_sel,',');
				--
			  exception
                when others then 
                  PKGERR.RAISE_ERROR(g_name, w_procnm,'143', w_pm);
              end;
              --
            when 2 then
              --
              declare
                w_pm varchar2(400) := jd_rec.keyclnm;
              begin
                w_clnms2 := GET_COLUMNS(jd_rec.keyclnm);
                --
				--dbms_output.put_line('test section 2 with case');
				--
                w_frm := w_frm||', ';
                --
                for i in 1..w_clnms2.count loop
				  declare
				    w_txt varchar2(4000);
				  begin
				    if w_cmbcnt = 1 then
                       w_txt := ' ';
                    else
                       w_txt := '      ';
                    end if;
                    --
                    w_txt := w_txt||'and   '||w_str1||'.'||w_clnms1(i)||' = '||w_str||'.'||w_clnms2(i)||w_lnbrk;
					--
					DBMS_LOB.WRITEAPPEND(w_whr, length(w_txt), w_txt);
				  exception
                    when others then 
                      PKGERR.RAISE_ERROR(g_name, w_procnm,'148', w_pm);
				  end;
                end loop;
                --
              exception
                when others then 
                  PKGERR.RAISE_ERROR(g_name, w_procnm,'120', w_pm);
              end;   
              --
            else
              --
              declare
                w_pm varchar2(400) := jd_rec.keyclnm;
              begin
                w_clnms2 := GET_COLUMNS(jd_rec.keyclnm);
                --
				--dbms_output.put_line('test section else with case');
                w_frm := w_frm||', ';
                --
                for i in 1..w_clnms2.count loop
				  declare
				    w_txt varchar2(4000);
				  begin
                    if w_cmbcnt = 1 then
                       w_txt := w_txt||' ';
                    else
                       w_txt := w_txt||'      ';
                    end if;
                    --
                    w_txt := w_txt||'and   '||w_str1||'.'||w_clnms1(i)||' = '||w_str||'.'||w_clnms2(i)||w_lnbrk;
					--
					DBMS_LOB.WRITEAPPEND(w_whr, length(w_txt), w_txt);
				  exception
				    when others then
					  PKGERR.RAISE_ERROR(g_name, w_procnm,'149', w_pm);
				  end;
                end loop;
                --
              exception
                when others then 
                  PKGERR.RAISE_ERROR(g_name, w_procnm,'121', w_pm);
              end;
            end case;
            --
            declare
			  w_pm varchar2(400) := jd_rec.keyclnm;
              w_flg varchar2(1) := 'Y';
            begin
			  if nvl(jd_rec.trgkeyflg,'N') = 'N' then
                declare
				    w_txt1 varchar2(4000);
					w_txt2 varchar2(4000);
				begin
                    w_txt1 := '            ';
                    w_txt2 := '            ';
                    --
				    w_txt1 := w_txt1||'w_trgtb1(i1).'||jd_rec.trgclnm||' := w_csr'||w_cmbcnt||
                            case when w_cmbcnt = 1 then '_tab(i).' else '_rec.' end ||jd_rec.valclnm||';'||w_lnbrk;
                    w_txt2 := w_txt2||'w_trgtb2(i2).'||jd_rec.trgclnm||' := w_csr'||w_cmbcnt||
                            case when w_cmbcnt = 1 then '_tab(i).' else '_rec.' end ||jd_rec.valclnm||';'||w_lnbrk;
                    --
					DBMS_LOB.WRITEAPPEND(w_asgn1, length(w_txt1), w_txt1);
					DBMS_LOB.WRITEAPPEND(w_asgn2, length(w_txt2), w_txt2);
				exception
				  when others then
				    PKGERR.RAISE_ERROR(g_name, w_procnm,'150', w_pm);
				end;
				--
				declare
				  w_txt varchar2(4000);
				begin
				  if w_cmbcnt = 1 then
                     case 
                     when upper(substr(jd_rec.trgcldtyp,1,3)) in ('STR','NUM','MON','FLO') then
                       w_txt  := w_txt||'         or nvl(w_trgrec.'||jd_rec.trgclnm||',''-1'') != nvl(w_csr'||w_cmbcnt||'_tab(i).'||jd_rec.valclnm||',''-1'')';
                     when upper(substr(jd_rec.trgcldtyp,1,4)) in ('DATE','TIME') then
                       w_txt  := w_txt||'         or nvl(w_trgrec.'||jd_rec.trgclnm||',sysdate+10000) != nvl(w_csr'||w_cmbcnt||'_tab(i).'||jd_rec.valclnm||',sysdate+10000)';
                     else
                       w_txt  := w_txt||'         or w_trgrec.'||jd_rec.trgclnm||' != w_csr'||w_cmbcnt||'_tab(i).'||jd_rec.valclnm;
                     end case;
                  else
                     case 
                     when upper(substr(jd_rec.trgcldtyp,1,3)) in ('STR','NUM','MON','FLO') then
                       w_txt  := w_txt||'         or nvl(w_trgrec.'||jd_rec.trgclnm||',''-1'') != nvl(w_csr'||w_cmbcnt||'_rec.'||jd_rec.valclnm||',''-1'')';
                     when upper(substr(jd_rec.trgcldtyp,1,4)) in ('DATE','TIME') then
                       w_txt  := w_txt||'         or nvl(w_trgrec.'||jd_rec.trgclnm||',sysdate+10000) != nvl(w_csr'||w_cmbcnt||'_rec.'||jd_rec.valclnm||',sysdate+10000)';
                     else
                       w_txt  := w_txt||'         or w_trgrec.'||jd_rec.trgclnm||' != w_csr'||w_cmbcnt||'_rec.'||jd_rec.valclnm;
                     end case;
                  end if;
                  --
                  w_txt  := w_txt||' '||w_lnbrk;
                  --
				  DBMS_LOB.WRITEAPPEND(w_dtwhr, length(w_txt), w_txt);
				exception
				  when others then
				    PKGERR.RAISE_ERROR(g_name, w_procnm,'151', w_pm);
				end;
			  else
			    w_flg := 'N';
              end if;
              --
              if w_flg = 'Y' then
			  	 w_sel := w_sel||','||w_str||'.'||jd_rec.valclnm;
              end if;
            exception
              when others then 
                PKGERR.RAISE_ERROR(g_name, w_procnm,'122', w_parm);
            end;
            --
            w_frm := w_frm||w_str;
            --
			declare
              w_pm varchar2(400) := jd_rec.trgclnm;
            begin
              if w_job_rec.trgtbtyp = 'DIM' and nvl(cmb_rec.scdtyp,1) = 1 then
			     DBMS_LOB.WRITEAPPEND(w_scd1upd
				                     ,length('         ,'||jd_rec.trgclnm||' = w_trgtb2(i2).'||jd_rec.trgclnm||w_lnbrk)
									 ,'         ,'||jd_rec.trgclnm||' = w_trgtb2(i2).'||jd_rec.trgclnm||w_lnbrk);
              end if;
			exception
              when others then 
                PKGERR.RAISE_ERROR(g_name, w_procnm,'144', w_pm);
            end;
            --
			if w_maprsqlcd is null and jd_rec.maprsqlcd is not null then
			   w_maprsqlcd := jd_rec.maprsqlcd;
			   --
			   DBMS_LOB.APPEND(w_maprsql, jd_rec.dwmaprsql);
			end if;
          exception
            when others then 
              PKGERR.RAISE_ERROR(g_name, w_procnm,'123', w_parm);
          end;
          --
        end loop;
        --
        if w_cmbcnt > 1 then
           for i in 1..w_clnms1.count loop
		     w_whr2 := w_whr2||'      and   sql1.'||w_clnms1(i)||' = w_csr1_tab(i).'||w_csr1key(i)||w_lnbrk;
           end loop;
        end if;
        --
		w_with   := rtrim(w_with,','||w_lnbrk);
        w_whr2   := rtrim(w_whr2,w_lnbrk);
        w_pkstr  := rtrim(w_pkstr,w_lnbrk);
        w_pkstr  := ltrim(w_pkstr,'and');
		--
		w_errstr := rtrim(w_errstr,'||');
		--
		declare
		  w_ln  integer := 0;
		begin
		  w_dtwhr := regexp_replace(w_dtwhr,'         or','',1,1);
		  --
		  w_ln  := DBMS_LOB.GETLENGTH(w_whr);
		  DBMS_LOB.TRIM(w_whr,w_ln-1);
		exception
		  when others then
		    PKGERR.RAISE_ERROR(g_name, w_procnm,'152a', w_parm);
		end;
		--
		declare
		  w_ln  integer := 0;
		begin 
		--
		  w_ln  := DBMS_LOB.GETLENGTH(w_scd1upd);
		  if nvl(w_ln,0) > 0 then
		     DBMS_LOB.TRIM(w_scd1upd,w_ln-1);
		  end if;
		  --
		exception
		  when others then
		    PKGERR.RAISE_ERROR(g_name, w_procnm,'152b', w_parm);
		end;
		--
		begin
		  w_asgn1 := regexp_replace(w_asgn1, '            ','',1,1);
		  w_asgn2 := regexp_replace(w_asgn2, '            ','',1,1);
		exception
		  when others then
		    PKGERR.RAISE_ERROR(g_name, w_procnm,'152c', w_parm);
		end;
		--
		declare
		  w_ln  integer := 0;
		begin
		  --
		  w_ln  := DBMS_LOB.GETLENGTH(w_dtwhr);
		  DBMS_LOB.TRIM(w_dtwhr,w_ln-1);
		  --
        exception
		  when others then
		    PKGERR.RAISE_ERROR(g_name, w_procnm,'152', w_parm);
		end;
		--
        declare
          w_text  varchar2(32767) := '';
        begin
          if w_cmbcnt = 1 then
            begin
			  if w_maprsqlcd is null then
                 w_text := ' '||w_with||w_lnbrk||
                           ' '||w_sel||w_lnbrk||
                           ' '||w_frm||w_lnbrk||' ';
				 --
				 DBMS_LOB.WRITEAPPEND(w_plsql1, length(w_text), w_text);
				 DBMS_LOB.APPEND(w_plsql1, w_whr);
				 --
				 w_text := ';'||w_lnbrk||w_lnbrk||
                           ' type csr'||w_cmbcnt||'_tabtyp is table of csr'||w_cmbcnt||'%rowtype index by binary_integer;'||w_lnbrk||
                           ' w_csr'||w_cmbcnt||'_tab csr'||w_cmbcnt||'_tabtyp;'||w_lnbrk||w_lnbrk;
                 DBMS_LOB.WRITEAPPEND(w_plsql1, length(w_text), w_text);
			  else
			     w_text := ' '||w_cursor||w_lnbrk||
				           ' '||regexp_replace(w_sel,'sql[0-9]{1,}','sql1')||w_lnbrk||
						   ' from (';
			     --
				 DBMS_LOB.WRITEAPPEND(w_plsql1, length(w_text), w_text);
				 DBMS_LOB.APPEND(w_plsql1,w_maprsql);
				 --
				 w_text := ' ) sql1;'||w_lnbrk||w_lnbrk||
                           ' type csr'||w_cmbcnt||'_tabtyp is table of csr'||w_cmbcnt||'%rowtype index by binary_integer;'||w_lnbrk||
                           ' w_csr'||w_cmbcnt||'_tab csr'||w_cmbcnt||'_tabtyp;'||w_lnbrk||w_lnbrk;
			     --
				 DBMS_LOB.WRITEAPPEND(w_plsql1, length(w_text), w_text);
				 --
			  end if;
            exception
              when others then
                PKGERR.RAISE_ERROR(g_name, w_procnm,'124', w_parm);
            end;
            --
			begin
              w_text := 'begin'||w_lnbrk||
			            ' begin'||w_lnbrk||
				        '   select dwjoblogseq.nextval into w_joblogid from dual;'||w_lnbrk||
				        ' exception when others then'||w_lnbrk||
				        '   PKGERR.RAISE_ERROR(g_name, w_procnm,'''||to_char(w_errnbr)||''', w_parm);'||w_lnbrk||
				        ' end;'||w_lnbrk||w_lnbrk||
				        ' declare'||w_lnbrk||
						'   cursor dwt_csr is'||w_lnbrk||
						'   select prcid, sessionid, param1, param2, param3, param4, param5'||w_lnbrk||
						'          ,param6, param7, param8, param9, param10'||w_lnbrk||
						'   from DWPRCLOG'||w_lnbrk||
						'   where status = ''IP'''||w_lnbrk||
						'   and   mapref = '''||w_job_rec.mapref||''''||w_lnbrk||
						'   and   sessionid = SYS_CONTEXT(''USERENV'',''SESSIONID'');'||w_lnbrk||
				        ' begin'||w_lnbrk||
						'   open dwt_csr;'||w_lnbrk||
						'   fetch dwt_csr into w_prcid, w_sessionid, dwt_param1, dwt_param2, dwt_param3, dwt_param4, dwt_param5'||w_lnbrk||
						'        ,dwt_param6, dwt_param7, dwt_param8, dwt_param9, dwt_param10;'||w_lnbrk||
						'   close dwt_csr;'||w_lnbrk||
				        ' exception when others then'||w_lnbrk||
				        '   PKGERR.RAISE_ERROR(g_name, w_procnm,'''||to_char(w_errnbr+1)||''', w_parm);'||w_lnbrk||
				        ' end;'||w_lnbrk||
                        ' --'||w_lnbrk;
              DBMS_LOB.WRITEAPPEND(w_plsql2, length(w_text), w_text);
			  --
			  w_errnbr := w_errnbr + 2;
            exception
              when others then
                PKGERR.RAISE_ERROR(g_name, w_procnm,'145', w_parm);
            end;
			--
            begin
              w_errnbr := w_errnbr + 1;
			  w_fstcnt := w_cmbcnt;
			  w_text := ' open csr'||w_cmbcnt||';'||w_lnbrk||
                        ' loop'||w_lnbrk||
                        '  w_csr'||w_cmbcnt||'_tab := csr'||w_cmbcnt||'_tabtyp();'||w_lnbrk||
                        '  w_trgtb1 := trgtbtyp();'||w_lnbrk||
                        '  w_trgtb2 := trgtbtyp();'||w_lnbrk||
                        '  w_trgtb3 := keytbtyp();'||w_lnbrk||
                        '  fetch csr'||w_cmbcnt||' bulk collect into w_csr'||w_cmbcnt||'_tab limit '||
                         nvl(w_job_rec.blkprcrows,w_limit)||';'||w_lnbrk||
                        '  exit when w_csr'||w_cmbcnt||'_tab.count = 0;'||w_lnbrk||
                        '  --'||w_lnbrk||
                        '  w_srccnt := w_srccnt + w_csr'||w_cmbcnt||'_tab.count;'||w_lnbrk||
                        '  --'||w_lnbrk||
                        '  for i in 1..w_csr'||w_cmbcnt||'_tab.count loop'||w_lnbrk||
                        '    --'||w_lnbrk;
              DBMS_LOB.WRITEAPPEND(w_plsql2, length(w_text), w_text);
            exception
              when others then
                PKGERR.RAISE_ERROR(g_name, w_procnm,'125', w_parm);
            end;
            --
            begin
			  w_errnbr := w_errnbr + 1;
			  w_text   := null;
			  if w_job_rec.trgtbtyp = 'DIM' then
                 w_text := '    declare'||w_lnbrk||
                           '      cursor trg_cur is '||w_lnbrk||
                           '      select * from '||w_job_rec.tbnam||' dtb'||w_lnbrk||
                           '      where curflg = ''Y'' '||w_lnbrk||
                           '      and '||w_pkstr||';'||w_lnbrk;
              end if;
			  --
			  w_text := w_text || '    begin'||w_lnbrk;
			  --
			  DBMS_LOB.WRITEAPPEND(w_plsql2, length(w_text), w_text);
			  --
			  if w_job_rec.trgtbtyp != 'DIM' then
			     w_text := '      -- Fact table process.'||w_lnbrk||
                           '      ';
			     DBMS_LOB.WRITEAPPEND(w_plsql2, length(w_text), w_text);
				 DBMS_LOB.APPEND(w_plsql2, w_asgn1);
              else
			     w_text := '      -- Dimension table process.'||w_lnbrk||
				           '      w_trgrec := null;'||w_lnbrk||
                           '      open trg_cur;'||w_lnbrk||
                           '      fetch trg_cur into w_trgrec;'||w_lnbrk||
                           '      close trg_cur;'||w_lnbrk||
                           '      --'||w_lnbrk||
                           '      if w_trgrec.skey is null then'||w_lnbrk||
						   '            if i1_cnt != i then'||w_lnbrk||
                           '               i1 := i1 + 1;'||w_lnbrk||
                           '			   i1_cnt := i;'||w_lnbrk||
                           '            end if;'||w_lnbrk||
                           '         ';
				 DBMS_LOB.WRITEAPPEND(w_plsql2, length(w_text), w_text);
				 DBMS_LOB.APPEND(w_plsql2, w_asgn1);
				 --
				 declare
				   w_ln integer := 0;
				 begin
				   w_ln := DBMS_LOB.GETLENGTH(w_dtwhr);
				   --
				   case when nvl(w_ln,0) > 0 then
                      w_text := '      else'||w_lnbrk||
                                '         if ';
					  --
					  DBMS_LOB.WRITEAPPEND(w_plsql2,length(w_text), w_text);
					  DBMS_LOB.APPEND(w_plsql2, w_dtwhr);
					  --
					  w_text := ' then'||w_lnbrk;
					  --
				  	  DBMS_LOB.WRITEAPPEND(w_plsql2,length(w_text), w_text);
				      --
				  	case 
                      when nvl(cmb_rec.scdtyp,1) = 2 then
                        w_text := '            -- SCD-II processing.'||w_lnbrk||
				  		        '            if i1_cnt != i then'||w_lnbrk||
                                  '               i1 := i1 + 1;'||w_lnbrk||
                                  '			     i1_cnt := i;'||w_lnbrk||
                                  '            end if;'||w_lnbrk||
				  			    '            if i3_cnt != i then'||w_lnbrk||
                                  '               i3 := i3 + 1;'||w_lnbrk||
                                  '			     i3_cnt := i;'||w_lnbrk||
                                  '            end if;'||w_lnbrk||
                                  '            w_trgtb3(i3) := w_trgrec.skey;'||w_lnbrk||
                                  '            w_trgtb1(i1) := w_trgrec;'||w_lnbrk||
                                  '            ';
                        DBMS_LOB.WRITEAPPEND(w_plsql2, length(w_text), w_text);
				        DBMS_LOB.APPEND(w_plsql2, w_asgn1);
				        --
				      when nvl(cmb_rec.scdtyp,1) = 1 then
                        w_text := '            -- SCD-I processing.'||w_lnbrk||
				  			    '            if i2_cnt != i then'||w_lnbrk||
                                  '               i2 := i2 + 1;'||w_lnbrk||
                                  '			     i2_cnt := i;'||w_lnbrk||
                                  '            end if;'||w_lnbrk||
                                  '            w_trgtb2(i2) := w_trgrec;'||w_lnbrk||
                                  '            ';
				  	    --
				  	    DBMS_LOB.WRITEAPPEND(w_plsql2, length(w_text), w_text);
				        DBMS_LOB.APPEND(w_plsql2, w_asgn2);
                        --
                      else
                        w_text := '            -- New record.'||w_lnbrk||
                                  '            ';
				  	    --
				  	    DBMS_LOB.WRITEAPPEND(w_plsql2, length(w_text), w_text);
				        DBMS_LOB.APPEND(w_plsql2, w_asgn1);
				  	  --
                      end case;
				  	--
				  	w_text := '         end if;'||w_lnbrk;
				  	--
				  	DBMS_LOB.WRITEAPPEND(w_plsql2, length(w_text), w_text);
                   end case;
				   --
				 exception
				   when others then
				     PKGERR.RAISE_ERROR(g_name, w_procnm,'153', w_parm);
				 end;
				 --
				 w_text := '      end if;'||w_lnbrk;
			     --
                 DBMS_LOB.WRITEAPPEND(w_plsql2, length(w_text), w_text);
			  end if;
			  --
              w_text := '    exception when others then'||w_lnbrk||
                        '      PKGERR.RAISE_ERROR(g_name, w_procnm,'''||to_char(w_errnbr)||''', w_parm);'||w_lnbrk||
                        '    end;'||w_lnbrk;
			  --
              DBMS_LOB.WRITEAPPEND(w_plsql2, length(w_text), w_text);
            exception
              when others then
                PKGERR.RAISE_ERROR(g_name, w_procnm,'126', w_parm);
            end;
            --
          else
             begin
			   w_text := '    --'||w_lnbrk||
                         '    declare'||w_lnbrk;
               DBMS_LOB.WRITEAPPEND(w_plsql3, length(w_text), w_text);
               --			   
			   if w_maprsqlcd is null then
                  w_text := '      '||replace(w_with,' with','      with')||w_lnbrk||
                            '      '||w_sel||w_lnbrk||
                            '      '||w_frm||w_lnbrk||
                            '      ';
				  --
				  DBMS_LOB.WRITEAPPEND(w_plsql3, length(w_text), w_text);
				  DBMS_LOB.APPEND(w_plsql3, w_whr);
				  DBMS_LOB.WRITEAPPEND(w_plsql3, length(w_lnbrk||w_whr2), w_lnbrk||w_whr2);
				  --
				  w_text := ';'||w_lnbrk||w_lnbrk||
                            '      w_csr'||w_cmbcnt||'_rec csr'||w_cmbcnt||'%rowtype;'||w_lnbrk||w_lnbrk;
                  DBMS_LOB.WRITEAPPEND(w_plsql3, length(w_text), w_text);
			   else
			     w_text := ' '||w_cursor||w_lnbrk||
				           ' '||regexp_replace(w_sel,'sql[0-9]{1,}','sql1')||w_lnbrk||
						   ' from (';
				 DBMS_LOB.WRITEAPPEND(w_plsql3, length(w_text), w_text);
				 DBMS_LOB.APPEND(w_plsql3,w_maprsql);
				 w_text := ' ) sql1'||w_lnbrk||
				           regexp_replace(w_whr2,'and','where',1,1)||' ;'||w_lnbrk||w_lnbrk||
				           '      w_csr'||w_cmbcnt||'_rec csr'||w_cmbcnt||'%rowtype;'||w_lnbrk||w_lnbrk;
				 DBMS_LOB.WRITEAPPEND(w_plsql3, length(w_text), w_text);
			   end if;
             exception
               when others then
                 PKGERR.RAISE_ERROR(g_name, w_procnm,'127', w_parm);
             end;
             --
             begin
               w_errnbr := w_errnbr + 1;
               w_text := '    begin'||w_lnbrk||
                         '      open csr'||to_char(w_cmbcnt)||';'||w_lnbrk||
                         '      fetch csr'||to_char(w_cmbcnt)||' into w_csr'||w_cmbcnt||'_rec;'||w_lnbrk||
                         '      close csr'||to_char(w_cmbcnt)||';'||w_lnbrk||
                         '      --'||w_lnbrk;
			   --
			   DBMS_LOB.WRITEAPPEND(w_plsql3, length(w_text), w_text);
			   --
			   if w_job_rec.trgtbtyp != 'DIM' then
			      w_text := '      -- Fact table process.'||w_lnbrk||
                            '      ';
                  --
                  DBMS_LOB.WRITEAPPEND(w_plsql3, length(w_text), w_text);
				  DBMS_LOB.APPEND(w_plsql3, w_asgn1);
			      --
			   else
			      w_text := '      if w_trgrec.skey is null then'||w_lnbrk||
							'         if i1_cnt != i then'||w_lnbrk||
                            '            i1 := i1 + 1;'||w_lnbrk||
                            '			 i1_cnt := i;'||w_lnbrk||
                            '         end if;'||w_lnbrk||
						    '         ';
				  --
				  DBMS_LOB.WRITEAPPEND(w_plsql3, length(w_text), w_text);
				  --
				  DBMS_LOB.APPEND(w_plsql3, w_asgn1);
				  --
				  declare
				    w_ln integer := 0;
				  begin
				    w_ln := DBMS_LOB.GETLENGTH(w_dtwhr);
					--
					case when nvl(w_ln,0) > 0 then
				      w_text := '      else'||w_lnbrk||
                                '         if ';
					  --
					  DBMS_LOB.WRITEAPPEND(w_plsql3, length(w_text), w_text);
					  DBMS_LOB.APPEND(w_plsql3, w_dtwhr);
					  --
					  w_text := ' then'||w_lnbrk;
					  --
					  DBMS_LOB.WRITEAPPEND(w_plsql3, length(w_text), w_text);
					  --
					  case 
                      when nvl(cmb_rec.scdtyp,1) = 2 then
					    w_text := '            -- SCD-II processing.'||w_lnbrk||
							      '            if i1_cnt != i then'||w_lnbrk||
                                  '               i1 := i1 + 1;'||w_lnbrk||
                                  '			    i1_cnt := i;'||w_lnbrk||
                                  '            end if;'||w_lnbrk||
                                  '            w_trgtb1(i1) := w_trgrec;'||w_lnbrk||
							      '            if i3_cnt != i then'||w_lnbrk||
                                  '               i3 := i3 + 1;'||w_lnbrk||
                                  '			    i3_cnt := i;'||w_lnbrk||
                                  '            end if;'||w_lnbrk||
                                  '            w_trgtb3(i3) := w_trgrec.skey;'||w_lnbrk||
                                  '            ';
					    --
					    DBMS_LOB.WRITEAPPEND(w_plsql3, length(w_text), w_text);
					    DBMS_LOB.APPEND(w_plsql3, w_asgn1);
                      when nvl(cmb_rec.scdtyp,1) = 1 then
                        w_text := '            -- SCD-I processing.'||w_lnbrk||
					  		    '            if i2_cnt != i then'||w_lnbrk||
                                  '               i2 := i2 + 1;'||w_lnbrk||
                                  '			    i2_cnt := i;'||w_lnbrk||
                                  '            end if;'||w_lnbrk||
                                  '            w_trgtb2(i2) := w_trgrec;'||w_lnbrk||
                                  '            ';
					    DBMS_LOB.WRITEAPPEND(w_plsql3, length(w_text), w_text);
					    DBMS_LOB.APPEND(w_plsql3,w_asgn2);
					    --
                      else
                        w_text := '            -- New record.'||w_lnbrk||
                                  '            ';
					    --
					    DBMS_LOB.WRITEAPPEND(w_plsql3, length(w_text), w_text);
					    DBMS_LOB.APPEND(w_plsql3, w_asgn1);
                      end case;
					  --
					end case;
					--
					w_text := '         end if;'||w_lnbrk;
					--
					DBMS_LOB.WRITEAPPEND(w_plsql3, length(w_text), w_text);
				  exception
				    when others then
					  PKGERR.RAISE_ERROR(g_name, w_procnm,'154', w_parm);
				  end;
				  --
				  w_text := '      end if;'||w_lnbrk;
                  --
				  DBMS_LOB.WRITEAPPEND(w_plsql3, length(w_text), w_text);
			      --
               end if;
               --			   
			   w_text := '    exception when others then'||w_lnbrk||
                         '      PKGERR.RAISE_ERROR(g_name, w_procnm,'''||to_char(w_errnbr)||''', w_parm);'||w_lnbrk||
                         '    end;'||w_lnbrk;
	           --
               DBMS_LOB.WRITEAPPEND(w_plsql3, length(w_text), w_text);
             exception
               when others then
                 PKGERR.RAISE_ERROR(g_name, w_procnm,'128', w_parm);
             end;
			 --
          end if;
          --
        exception
          when others then 
            PKGERR.RAISE_ERROR(g_name, w_procnm,'129', w_parm);
        end;
        --
		DBMS_LOB.FREETEMPORARY(w_whr);
		--
  	  exception
        when others then 
          PKGERR.RAISE_ERROR(g_name, w_procnm,'130', w_parm);
      end;
      --
	  DBMS_LOB.FREETEMPORARY(w_maprsql);
      --
    end loop;
    --
    if w_job_rec.trgtbtyp = 'DIM' then
	   DBMS_LOB.WRITEAPPEND(w_dtins1, length('CURFLG, FROMDT, TODT,'), 'CURFLG, FROMDT, TODT,');
	   DBMS_LOB.WRITEAPPEND(w_dtins2, length('''Y'',sysdate,w_hidt,'), '''Y'',sysdate,w_hidt,');
    end if;
    --
    DBMS_LOB.WRITEAPPEND(w_dtins1, length(' RECCRDT, RECUPDT'), ' RECCRDT, RECUPDT');
	DBMS_LOB.WRITEAPPEND(w_dtins2, length(' sysdate, sysdate'), ' sysdate, sysdate');
    --
    begin
      DBMS_LOB.APPEND(w_plsql, w_plsql1);
      DBMS_LOB.APPEND(w_plsql, w_plsql2);
      DBMS_LOB.APPEND(w_plsql, w_plsql3);
    exception
      when others then
        PKGERR.RAISE_ERROR(g_name, w_procnm,'131', w_parm);
    end;
    --
    begin
	  w_errnbr := w_errnbr + 1;
      w_text := '  end loop;'||w_lnbrk||w_lnbrk;
      DBMS_LOB.WRITEAPPEND(w_plsql, length(w_text), w_text);
    exception
      when others then
        PKGERR.RAISE_ERROR(g_name, w_procnm,'132', w_parm);
    end;
    --
    if w_job_rec.trgtbtyp = 'DIM' then
       begin
         w_errnbr := w_errnbr + 1;
		 w_text := '  if w_trgtb3.count > 0 then'||w_lnbrk||
                   '     -- Ending changed records'||w_lnbrk||
                   '     declare'||w_lnbrk||
                   '       e integer := 0;'||w_lnbrk||
                   '     begin'||w_lnbrk||
                   '       forall i in w_trgtb3.first .. w_trgtb3.last save exceptions'||w_lnbrk||
                   '         update '||w_job_rec.tbnam||w_lnbrk||
                   '         set    curflg = ''N'''||w_lnbrk||
                   '               ,todt = trunc(sysdate)'||w_lnbrk||
                   '               ,recupdt = sysdate'||w_lnbrk||
                   '         where skey = w_trgtb3(i);'||w_lnbrk||
                   '     exception when blk_err then'||w_lnbrk||
                   '       declare'||w_lnbrk||
                   '           w_msg varchar2(400) := '''';'||w_lnbrk||
                   '       begin'||w_lnbrk||
                   '         w_errcnt := w_errcnt + sql%bulk_exceptions.count;'||w_lnbrk||
                   '         for e in 1..sql%bulk_exceptions.count loop'||w_lnbrk||
                   '           w_msg := sqlerrm(-sql%bulk_exceptions(e).error_code);'||w_lnbrk||
				   '           PKGDWPRC.LOG_JOB_ERROR(w_joblogid, w_sessionid, w_prcid, w_sysdt, '||to_char(w_job_rec.jobid)||w_lnbrk||
				   '                                 ,'''||w_job_rec.mapref||''',''ERR'', w_msg,''SCD-II update failed.'''||w_lnbrk||
				   '                                 ,w_trgtb3(e));'||w_lnbrk||
                   '         end loop;'||w_lnbrk||
                   '       exception when others then'||w_lnbrk||
                   '         PKGERR.RAISE_ERROR(g_name, w_procnm,'''||to_char(w_errnbr)||''', w_parm);'||w_lnbrk||
                   '       end;'||w_lnbrk||
                   '     when others then'||w_lnbrk||
                   '       PKGERR.RAISE_ERROR(g_name, w_procnm,'''||to_char(w_errnbr+1)||''', w_parm);'||w_lnbrk||
                   '      end;'||w_lnbrk||
                   '  end if;'||w_lnbrk||w_lnbrk;
         DBMS_LOB.WRITEAPPEND(w_plsql, length(w_text), w_text);
		 w_errnbr := w_errnbr + 1;
       exception
         when others then
           PKGERR.RAISE_ERROR(g_name, w_procnm,'133', w_parm);
       end;
       --
       begin
	     w_errnbr := w_errnbr + 1;
         w_text := '  if w_trgtb2.count > 0 then'||w_lnbrk||
                   '     -- Ending changed records'||w_lnbrk||
                   '     declare'||w_lnbrk||
                   '       e integer := 0;'||w_lnbrk||
                   '     begin'||w_lnbrk||
                   '       forall i in w_trgtb2.first .. w_trgtb2.last save exceptions'||w_lnbrk||
                   '         update '||w_job_rec.tbnam||w_lnbrk||
                   '         set    recupdt = sysdate'||w_lnbrk;
         --
		 DBMS_LOB.WRITEAPPEND(w_plsql, length(w_text), w_text);
		 DBMS_LOB.APPEND(w_plsql, w_scd1upd);
		 --
		 w_text := w_lnbrk||
                   '         where skey = w_trgtb2(i).skey;'||w_lnbrk||
                   '     exception when blk_err then'||w_lnbrk||
                   '       declare'||w_lnbrk||
                   '           w_msg varchar2(400) := '''';'||w_lnbrk||
                   '       begin'||w_lnbrk||
                   '         w_errcnt := w_errcnt + sql%bulk_exceptions.count;'||w_lnbrk||
                   '         for e in 1..sql%bulk_exceptions.count loop'||w_lnbrk||
                   '           w_msg := sqlerrm(-sql%bulk_exceptions(e).error_code);'||w_lnbrk||
				   '           PKGDWPRC.LOG_JOB_ERROR(w_joblogid, w_sessionid, w_prcid, w_sysdt, '||to_char(w_job_rec.jobid)||w_lnbrk||
				   '                                 ,'''||w_job_rec.mapref||''',''ERR'', w_msg,''SCD-I update failed.'''||w_lnbrk||
				   '                                 ,w_trgtb2(e).skey);'||w_lnbrk||
                   '         end loop;'||w_lnbrk||
                   '       exception when others then'||w_lnbrk||
                   '         PKGERR.RAISE_ERROR(g_name, w_procnm,'''||to_char(w_errnbr)||''', w_parm);'||w_lnbrk||
                   '       end;'||w_lnbrk||
                   '     when others then'||w_lnbrk||
                   '       PKGERR.RAISE_ERROR(g_name, w_procnm,'''||to_char(w_errnbr+1)||''', w_parm);'||w_lnbrk||
                   '      end;'||w_lnbrk||
                   '  end if;'||w_lnbrk||w_lnbrk;
         DBMS_LOB.WRITEAPPEND(w_plsql, length(w_text), w_text);
		 w_errnbr := w_errnbr + 1;
       exception
         when others then
           PKGERR.RAISE_ERROR(g_name, w_procnm,'134', w_parm);
       end;
    end if;
    --
    begin
	  w_errnbr := w_errnbr + 1;
      w_text := '  if w_trgtb1.count > 0 then'||w_lnbrk||
                '     -- Insert new record.'||w_lnbrk||
                '     declare'||w_lnbrk||
                '       e integer := 0;'||w_lnbrk||
                '     begin'||w_lnbrk||
                '       forall i in w_trgtb1.first .. w_trgtb1.last save exceptions'||w_lnbrk||
                '         insert into '||w_job_rec.tbnam||' ('||w_lnbrk||
                '         skey,';
	  DBMS_LOB.WRITEAPPEND(w_plsql, length(w_text), w_text);
	  DBMS_LOB.APPEND(w_plsql,w_dtins1);
	  w_text := ')'||w_lnbrk||
                '         values ('||w_job_rec.tbnam||'_SEQ.nextval,';
	  DBMS_LOB.WRITEAPPEND(w_plsql, length(w_text), w_text);
	  DBMS_LOB.APPEND(w_plsql,w_dtins2);
	  w_text := ');'||w_lnbrk||
                '     exception when blk_err then'||w_lnbrk||
                '       declare'||w_lnbrk||
                '         w_msg varchar2(400) := '''';'||w_lnbrk||
                '       begin'||w_lnbrk||
                '         w_errcnt := w_errcnt + sql%bulk_exceptions.count;'||w_lnbrk||
                '         for e in 1..sql%bulk_exceptions.count loop'||w_lnbrk||
                '           w_msg := sqlerrm(-sql%bulk_exceptions(e).error_code);'||w_lnbrk||
			    '           PKGDWPRC.LOG_JOB_ERROR(w_joblogid, w_sessionid, w_prcid, w_sysdt, '||to_char(w_job_rec.jobid)||w_lnbrk||
				'                                 ,'''||w_job_rec.mapref||''',''ERR'', w_msg,''Insert new record failed.'''||w_lnbrk||
				'                                 ,'||w_errstr||');'||w_lnbrk||
                '         end loop;'||w_lnbrk||
                '       exception when others then'||w_lnbrk||
                '         PKGERR.RAISE_ERROR(g_name, w_procnm,'''||to_char(w_errnbr)||''', w_parm);'||w_lnbrk||
                '       end;'||w_lnbrk||
                '     when others then'||w_lnbrk||
                '       PKGERR.RAISE_ERROR(g_name, w_procnm,'''||to_char(w_errnbr+1)||''', w_parm);'||w_lnbrk||
                '     end;'||w_lnbrk||
                '  end if;'||w_lnbrk||w_lnbrk;
       DBMS_LOB.WRITEAPPEND(w_plsql, length(w_text), w_text);
	   w_errnbr := w_errnbr + 1;
     exception
       when others then
         PKGERR.RAISE_ERROR(g_name, w_procnm,'135', w_parm);
     end;
	 --
	 DBMS_LOB.FREETEMPORARY(w_dtins1);
	 DBMS_LOB.FREETEMPORARY(w_dtins2);
     --
     begin
	   w_errnbr := w_errnbr + 1;
       w_text := '  w_trgcnt := w_trgcnt + nvl(w_trgtb1.count,0) + nvl(w_trgtb2.count,0);'||w_lnbrk||
				 '  commit;'||w_lnbrk||
	             ' end loop;'||w_lnbrk||w_lnbrk||
				 ' close csr'||w_fstcnt||';'||w_lnbrk||w_lnbrk; 
       DBMS_LOB.WRITEAPPEND(w_plsql, length(w_text), w_text);
     exception
       when others then
         PKGERR.RAISE_ERROR(g_name, w_procnm,'136', w_parm);
     end;
     --
     begin
       -- Log record creation
	   w_errnbr := w_errnbr + 1;
       w_text := ' begin'||w_lnbrk||
                 '   PKGDWPRC.CREATE_JOB_LOG(w_joblogid'||w_lnbrk||
				 '                          ,w_sessionid'||w_lnbrk||
				 '                          ,w_prcid'||w_lnbrk||
                 '                          ,w_sysdt'||w_lnbrk||
                 '                          ,'''||w_job_rec.mapref||''''||w_lnbrk||
                 '                          ,'||w_job_rec.jobid||w_lnbrk||
                 '                          ,w_srccnt'||w_lnbrk||
                 '                          ,w_trgcnt'||w_lnbrk||
                 '                          ,w_errcnt);'||w_lnbrk||
				 '   commit;'||w_lnbrk||
                 ' exception when others then'||w_lnbrk||
                 '   PKGERR.RAISE_ERROR(g_name, w_procnm,'''||to_char(w_errnbr)||''', w_parm);'||w_lnbrk||
                 ' end;'||w_lnbrk||w_lnbrk;
       DBMS_LOB.WRITEAPPEND(w_plsql, length(w_text), w_text);
     exception
       when others then
         PKGERR.RAISE_ERROR(g_name, w_procnm,'137', w_parm);
     end;
	 --
     begin
	   w_errnbr := w_errnbr + 1;
       w_text := 'exception when others then'||w_lnbrk||
                 '  declare'||w_lnbrk||
                 '    w_msg varchar2(4000) := substr(sqlerrm,1,4000);'||w_lnbrk||
                 '  begin'||w_lnbrk||
                 '    PKGDWPRC.LOG_JOB_ERROR(w_joblogid, w_sessionid, w_prcid, w_sysdt, '||to_char(w_job_rec.jobid)||w_lnbrk||
				 '                          ,'''||w_job_rec.mapref||''',''ERR'', w_msg,''Job processing failed.'''||w_lnbrk||
				 '                          ,w_parm );'||w_lnbrk||
				 '    commit;'||w_lnbrk||
                 '  exception when others then'||w_lnbrk||
				 '    PKGERR.RAISE_ERROR(g_name, w_procnm,'''||to_char(w_errnbr)||''', w_parm);'||w_lnbrk||	 
                 '  end;'||w_lnbrk||				 
                 '  PKGERR.RAISE_ERROR(g_name, w_procnm,'''||to_char(w_errnbr+1)||''', w_parm);'||w_lnbrk||
                 'end;';
       DBMS_LOB.WRITEAPPEND(w_plsql, length(w_text), w_text);
     exception
       when others then
         PKGERR.RAISE_ERROR(g_name, w_procnm,'138', w_parm);
     end;
     --
     -- dbms_output.put_line(w_plsql);
     --
     -- Create job flow record.
     --
     declare
       w_parm varchar2(400) := substr('jobId='||w_job_rec.jobid||
                                     ' MapRef='||w_job_rec.mapref||
                                     ' TrgSchm='||w_job_rec.trgschm||
                                     ' TrgTbTyp='||w_job_rec.trgtbtyp||
                                     ' TrgTbNm='||w_job_rec.trgtbnm, 1, 400);
       cursor flw_csr is
	   select jobflwid, dwlogic
	   from   dwjobflw
	   where  mapref = w_job_rec.mapref
	   and    curflg = 'Y';
	   --
	   w_flw_rec flw_csr%rowtype := null;
	   --
	   w_res   integer := null;
     begin
	   open flw_csr;
	   fetch flw_csr into w_flw_rec;
	   close flw_csr;
	   --
	   if w_flw_rec.jobflwid is not null then
	      w_res := DBMS_LOB.COMPARE(w_plsql, w_flw_rec.dwlogic);
	   else
	      w_res := 1;
	   end if;
	   --
	   if w_res != 0 then
          --
          -- update existing flow record to inactive.
          -- 
          if w_flw_rec.jobflwid is not null then
             update dwjobflw
             set    curflg = 'N'
             where  curflg = 'Y'
             and    jobflwid = w_flw_rec.jobflwid;
		  end if;
          --
          -- Create the updated flow record.
          --
          insert into dwjobflw (jobflwid,jobid,mapref,trgschm,trgtbtyp,trgtbnm
                              ,dwlogic,stflg,recrdt,recupdt,curflg)
          values (dwjobflwseq.nextval, w_job_rec.jobid, w_job_rec.mapref, w_job_rec.trgschm, w_job_rec.trgtbtyp, w_job_rec.trgtbnm
                 ,w_plsql, 'A', sysdate, sysdate, 'Y');
          --
	   end if;
     exception
       when others then 
         PKGERR.RAISE_ERROR(g_name, w_procnm,'139', w_parm);
     end;
     --
     begin
	   DBMS_LOB.FREETEMPORARY(w_plsql);
       DBMS_LOB.FREETEMPORARY(w_plsql1);
       DBMS_LOB.FREETEMPORARY(w_plsql2);
       DBMS_LOB.FREETEMPORARY(w_plsql3);
     exception
       when others then
         PKGERR.RAISE_ERROR(g_name, w_procnm,'140', w_parm);
     end;
     --
	 commit;
	 --
   exception
     when others then 
       PKGERR.RAISE_ERROR(g_name, w_procnm,'141', w_parm);
   end CREATE_JOB_FLOW;
   
end PKGDWJOB;
/
