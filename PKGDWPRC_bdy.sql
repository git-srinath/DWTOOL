--
-- Package for processing jobs.
--
/*
Last error: 157

Change history:
date        who              Remarks
----------- ---------------- ------------------------------------------------------------------
28-May-2025 Srinath C V      PROCESS_JOB_FLOW updated to use parameters and to log parameters.
02-Jun-2025 Srinath C V      PROCESS_JOB_FLOW updated to commit data inserted for job processing.
05-Jun-2025 Srinath C V      PROCESS_JOB_FLOW updated with additional 5 parameters.
							 Added to capture sessionid and mapping reference.
							 CREATE_JOB_LOG amended to add sessionid and PRCID parameters.
26-Jul-2025 Srinath C V      Added SCHEDULE_HISTORY_JOB_IMMEDIATE procedure.
01-Jul-2025 Srinath C V      STOP_RUNNING_JOB added.


*/
Create or replace package body PKGDWPRC is

	g_name constant varchar2(10) := 'PKGDWPRC';
	g_ver  constant varchar2(10) := 'V001';
	--
	-- Global types and variables for this package;
	--
	type clnmtb_typ is table of dwjobdtl.trgclnm%type index by binary_integer;
	--
	function VERSION return varchar is
	begin
	  return g_name||';'||g_ver;
	end VERSION;
	
	--
	-- Procedure to process historical data, created for BCCB DW project.
	--
	procedure PROCESS_HISTORICAL_DATA(p_mapref in dwjobflw.mapref%type
                                     ,p_strtdt in date
								     ,p_enddt  in date
	                                 ,p_tl_flg in varchar2) is
      --
	  w_procnm  varchar2(50) := 'PROCESS_HISTORICAL_DATA';
	  --
	  w_parm    varchar2(400) := substr('Mapref='||p_mapref||
	                                   ' TruncateLoadFlag = '||p_tl_flg,1,400);
      --
	  dwt_param1 varchar2(250) := null;
      w_dt       date          := null;
      w_enddt    date          := null;
	  --
	  w_trgschm  dwmapr.trgschm%type := null;
	  w_trgtbnm  dwmapr.trgtbnm%type := null;
	  w_text     varchar2(400) := 'truncate table ';
	  --
	  cursor c1 is
	  select prval
	  from  dwparams
	  where prtyp = 'HIST_LOAD'
	  and   prcd = 'DWT_PARAM1';
	  --
	  cursor c2 is
	  select trgschm, trgtbnm
	  from dwmapr
	  where mapref = p_mapref
	  and   curflg = 'Y';
	  --
    begin
	  --
	  open c1;
	  fetch c1 into dwt_param1;
	  close c1;
	  --
	  open c2;
	  fetch c2 into w_trgschm, w_trgtbnm;
	  close c2;
	  --
	  w_text := w_text||w_trgschm||'.'||w_trgtbnm||' drop storage';
	  --
	  dbms_output.put_line('w_text ='||w_text);
	  --
	  w_dt    := nvl(p_strtdt, to_date(dwt_param1,'DD-Mon-YYYY'));
	  w_enddt := nvl(p_enddt,  trunc(sysdate)-1);
      --
	  if p_tl_flg = 'Y' then
	     execute immediate 'truncate table '||w_trgschm||'.'||w_trgtbnm||' drop storage';
	  end if;
	  --
      while w_dt <= w_enddt loop
        --
        PKGDWPRC.PROCESS_JOB_FLOW(p_mapref => p_mapref
                                 ,p_param1 => to_char(w_dt,'DD-Mon-YYYY') );
    	--
	    w_dt := w_dt + 1;
	    --
	    commit;
	    --
      end loop;
	exception
	  when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'154', w_parm);
	end PROCESS_HISTORICAL_DATA;
	
    --
    -- Procedure to create job log record.
    --
    procedure CREATE_JOB_LOG(p_joblogid  in out dwjoblog.joblogid%type
                            ,p_sessionid in     dwjoblog.sessionid%type
                            ,p_prcid     in     dwjoblog.prcid%type
                         	,p_prcdt     in     dwjoblog.prcdt%type
	                        ,p_mapref    in     dwjoblog.mapref%type
							,p_jobid     in     dwjoblog.jobid%type
							,p_srcrows   in     dwjoblog.srcrows%type
							,p_trgrows   in     dwjoblog.trgrows%type
							,p_errrows   in     dwjoblog.errrows%type) is
	  --
	  w_procnm  varchar2(50) := 'CREATE_JOB_LOG';
	  --
	  w_parm    varchar2(400) := substr('Joblogid='||p_joblogid||
	                             ' Sessionid='||p_sessionid||
								 ' PrcId='||p_prcid||
	                             ' PrcDt='||to_char(p_prcdt,'DD-Mon-YYYY HH24:MI')||
	                             ' Mapref='||p_mapref||
	                             ' Jobid='||p_jobid||
	                             ' SrcRow='||p_srcrows||
	                             ' TrgRow='||p_trgrows||
								 ' ErrRow='||p_errrows,1,400);
	  --
	  w_joblogid dwjoblog.joblogid%type := p_joblogid;
	begin
	  --
	  if w_joblogid is null then
	     select DWJOBLOGSEQ.nextval
		 into   w_joblogid
		 from dual;
	  end if;
	  --
	  insert into dwjoblog(joblogid, prcdt, mapref, jobid, srcrows, trgrows, errrows, reccrdt, prcid, sessionid)
	  values (p_joblogid, p_prcdt, p_mapref, p_jobid, p_srcrows, p_trgrows, p_errrows, sysdate, p_prcid, p_sessionid);
	  --
	  p_joblogid := w_joblogid;
	  --
	exception
	  when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'101', w_parm);
	end CREATE_JOB_LOG;

   --
   -- Proedure to create job errors.
   --
   procedure LOG_JOB_ERROR(p_joblogid  in dwjoberr.joblogid%type
                          ,p_sessionid in dwjoberr.sessionid%type
                          ,p_prcid     in dwjoberr.prcid%type
						  ,p_prcdt     in dwjoberr.prcdt%type
						  ,p_jobid     in dwjoberr.jobid%type
						  ,p_mapref    in dwjoberr.mapref%type
						  ,p_errtyp    in dwjoberr.errtyp%type
						  ,p_dberrmsg  in dwjoberr.dberrmsg%type
						  ,p_errmsg    in dwjoberr.errmsg%type
						  ,p_keyvalue  in dwjoberr.keyvalue%type) is
	  --
	  w_procnm  varchar2(50) := 'LOG_JOB_ERROR';
	  --
	  w_parm    varchar2(400) := substr('Joblogid='||p_joblogid||
	                             ' Sessionid='||p_sessionid||
								 ' PrcId='||p_prcid||
	                             ' PrcDt='||to_char(p_prcdt,'DD-Mon-YYYY HH24:MI')||
								 ' JobId='||p_jobid||
	                             ' Mapref='||p_mapref||
	                             ' KeyVal='||p_keyvalue,1,400);						 
    begin
	  insert into
	  dwjoberr(errid, joblogid, sessionid, prcid, prcdt, jobid, mapref, errtyp
              ,dberrmsg, errmsg, keyvalue, reccrdt)
      values  (dwjoberrseq.nextval, p_joblogid, p_sessionid, p_prcid, p_prcdt, p_jobid, p_mapref, p_errtyp
              ,p_dberrmsg, p_errmsg, p_keyvalue, sysdate);
      --
	  begin
	    update dwprclog
		set    enddt = sysdate
			  ,status = 'FL'
			  ,recupdt = sysdate
			  ,msg = p_errmsg
		where prcid = p_prcid;
	  exception
	    when others then
		  PKGERR.RAISE_ERROR(g_name, w_procnm,'151', w_parm);
      end;
	exception
	  when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'150', w_parm);
	end LOG_JOB_ERROR;
						 
    --
	-- Procedure to process active job flow.
	--
    procedure PROCESS_JOB_FLOW(p_mapref  in dwjobflw.mapref%type
                              ,p_param1  in varchar2 default null
							  ,p_param2  in varchar2 default null
							  ,p_param3  in varchar2 default null
							  ,p_param4  in varchar2 default null
							  ,p_param5  in varchar2 default null
							  ,p_param6  in varchar2 default null
							  ,p_param7  in varchar2 default null
							  ,p_param8  in varchar2 default null
							  ,p_param9  in varchar2 default null
							  ,p_param10 in varchar2 default null) is
	  w_procnm  varchar2(50) := 'PROCESS_JOB_FLOW';
	  --
	  w_parm    varchar2(400) := substr('Mapref='||p_mapref,1,400);
	  --
	  cursor flw_csr is
	  select jobflwid, jobid, mapref, dwlogic
	  from  dwjobflw
	  where mapref = p_mapref
	  and   curflg = 'Y'
	  and   stflg  = 'A';
	  --
	  w_flw_rec  flw_csr%rowtype      := null;
	  w_prcid    dwprclog.prcid%type  := null;
	  w_prclog   dwprclog.prclog%type := null;
	  w_ssid     dwprclog.sessionid%type;
	  --
	begin
	  open  flw_csr;
	  fetch flw_csr into w_flw_rec;
	  close flw_csr;
	  --
	  w_ssid := SYS_CONTEXT('USERENV','SESSIONID');
	  --
	  if DBMS_LOB.getlength(w_flw_rec.dwlogic) > 0 then
	     --
	  	 if p_param1 is not null
		 or p_param2 is not null
		 or p_param3 is not null
		 or p_param4 is not null
		 or p_param5 is not null then
		    w_prclog := substr('Parameters used:'||
		                ' DWT_PARAM1='||substr(p_param1,1,250)||' ,'||
		                ' DWT_PARAM2='||substr(p_param2,1,250)||' ,'||
			            ' DWT_PARAM3='||substr(p_param3,1,250)||' ,'||
			            ' DWT_PARAM4='||substr(p_param4,1,250)||' ,'||
						' DWT_PARAM5='||substr(p_param4,1,250)||' ,'||
						' DWT_PARAM6='||substr(p_param4,1,250)||' ,'||
						' DWT_PARAM7='||substr(p_param4,1,250)||' ,'||
						' DWT_PARAM8='||substr(p_param4,1,250)||' ,'||
						' DWT_PARAM9='||substr(p_param4,1,250)||' ,'||
			            ' DWT_PARAM10='||substr(p_param5,1,250), 1, 4000);
         end if;
		 --
		 begin
		   insert into 
		   dwprclog(prcid, jobid, jobflwid, strtdt, status, reccrdt, recupdt, prclog, mapref, sessionid
		           ,param1, param2, param3, param4, param5, param6, param7, param8, param9, param10)
		   values(dwprclogseq.nextval, w_flw_rec.jobid, w_flw_rec.jobflwid, sysdate, 'IP',sysdate, sysdate, w_prclog
		         ,p_mapref, w_ssid
				 ,p_param1, p_param2, p_param3, p_param4, p_param5, p_param6, p_param7, p_param8, p_param9, p_param10)
		   returning prcid into w_prcid;
		   --
		   commit;
		 exception
		   when others then
		     PKGERR.RAISE_ERROR(g_name, w_procnm,'149', w_parm);
		 end;  
		 --
		 -- Process the data flow.
		 --
		 begin
		   execute immediate w_flw_rec.dwlogic;		 
		 exception
	       when others then
		     declare 
			   w_msg dwprclog.msg%type := substr(sqlerrm,1,400);
		     begin
			   update dwprclog
			   set    enddt = sysdate
			         ,status = 'FL'
					 ,recupdt = sysdate
					 ,msg = w_msg
			   where prcid = w_prcid;
			 exception
			   when others then
			     PKGERR.RAISE_ERROR(g_name, w_procnm,'102', w_parm);
		     end;
			 --
	         PKGERR.RAISE_ERROR(g_name, w_procnm,'103', w_parm);
	     end;
		 --
		 begin
		   update dwprclog
		   set    enddt = sysdate
		         ,status = 'PC'
				 ,recupdt = sysdate
		   where prcid = w_prcid;
		 exception
		   when others then
			 PKGERR.RAISE_ERROR(g_name, w_procnm,'104', w_parm);
		 end;
	  end if;
	  --
	  commit;
	  --
	exception
	  when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'105', w_parm);
	end PROCESS_JOB_FLOW;
	
	--
    -- Function to create DW scheduler record.
    --
    Function CREATE_JOB_SCHEDULE(p_mapref in dwjobsch.mapref%type
                                ,p_frqcd  in dwjobsch.frqcd%type
                                ,p_frqdd  in dwjobsch.frqdd%type
                                ,p_frqhh  in dwjobsch.frqhh%type
                                ,p_frqmi  in dwjobsch.frqmi%type
								,p_strtdt in dwjobsch.strtdt%type
								,p_enddt  in dwjobsch.enddt%type)
    return dwjobsch.jobschid%type is
	  w_procnm  varchar2(50) := 'CREATE_JOB_SCHEDULE';
	  --
	  w_parm    varchar2(400) := substr('Mapref='||p_mapref||
	                                   ' Frqcd='||p_frqcd||
									   ' Frqdd='||p_frqdd||
									   ' Frqhh='||p_frqhh||
									   ' Frqmi='||p_frqmi||
									   ' StrtDt='||to_char(p_strtdt,'DD-Mon-YYYY')||
									   ' EndDt='||to_char(p_enddt,'DD-Mon-YYYY'),1,400);
	  --
	  w_msg     varchar2(200) := null;
	  ----
	  cursor flw_csr is
	  select f.jobflwid, f.mapref, s.jobschid, s.frqcd, s.frqdd, s.frqhh, s.frqmi,
	         s.strtdt, s.enddt
	  from   dwjobflw f, dwjobsch s
	  where  f.mapref = p_mapref
	  and    f.curflg = 'Y'
	  and    f.stflg = 'A'
	  and    s.jobflwid(+) = f.jobflwid
	  and    s.mapref(+)   = f.mapref
	  and    s.curflg(+)   = 'Y';
	  --
	  w_flw_rec  flw_csr%rowtype        := null;
	  w_jobschid dwjobsch.jobschid%type := null;
	  w_chg      varchar2(1)            := 'Y';
	  --
	begin
	  case 
	  --
	  -- Mapping reference must be provided, this is basics of the tool.
	  --
	  when p_mapref is null then
	    w_msg := 'Mapping reference must be provided.';
	  --
	  -- The frequency code must be a valid listed value, others not allowed.
	  --
	  when nvl(p_frqcd,'#') not in ('ID','DL','WK','FN','MN','HY','YR') then
	    w_msg := 'Invalid frequency code (Valid: ID,DL,WK,FN,MN,HY,YR).';
	  --
	  -- For Weekly frequency the Day of the week must be the frequency day not calendar day.
	  --
	  when p_frqcd in ('FN','WK') and p_frqdd not in ('MON','TUE','WED','THU','FRI','SAT','SUN') then
		w_msg := 'Invalid Frequency Day. For Weekly/Fortlightly frequency, frequency day can be any one of "MON,TUE,WED,THU,FRI,SAT,SUN".';
	  --
	  -- For monthly frequency, the frequency day must be the calendar day.
	  -- For daily and intra-day frequency, frequency day is not required (can be ignored).
	  --
      when p_frqcd not in ('FN','WK','DL','ID') and nvl(p_frqdd,0) not between 1 and 31 then
	    w_msg := 'Invalid frequency day (Valid: 1 .. 31).';
	  --
	  -- The frequency hour must be in 24 hour clock format, others values not allowed.
	  -- Applicable to all types of frequency.
	  --
	  when nvl(p_frqhh,-1) not between 0 and 23 then
	    w_msg := 'Invalid frequency hour (valid: 0 .. 23).';
	  --
	  -- The frequency minute must be any value between 0 to 59, other values not allowed.
	  --
	  when nvl(p_frqmi,-1) not between 0 and 59 then
	    w_msg := 'Invalid frequency minute (valid: 0 .. 59).';
	  --
	  -- Start date represents the date from which schedule must start, it is mandtory information.
	  --
	  when p_strtdt is null then
	    w_msg := 'Schedule start date must be provided.';
	  --
	  -- Start date must not in the past.
	  --
	  when p_strtdt < trunc(sysdate) then
	    w_msg := 'Schedule start date must not be in past.';
	  --
	  -- End date must be after start date or can be blank/null to represent for-ever.
	  --
	  when p_enddt is not null and p_strtdt >= p_enddt then
	    w_msg := 'Schedule start date must be before shedule end date.'; 
	  else 
	    w_msg := null;
	  end case;
	  --
	  if w_msg is not null then
	     w_parm := w_parm||'::'||w_msg;
		 raise value_error;
	  end if;
	  --
	  begin 
	    open flw_csr;
		fetch flw_csr into w_flw_rec;
		close flw_csr;
		--
		w_jobschid := w_flw_rec.jobschid;
		--
	  exception 
	    when others then
		  PKGERR.RAISE_ERROR(g_name, w_procnm,'106', w_parm);
	  end;
	  --
	  if w_flw_rec.jobschid is not null then
	     if w_flw_rec.frqcd  != p_frqcd
		 or w_flw_rec.frqdd  != p_frqdd
		 or w_flw_rec.frqhh  != p_frqhh
		 or w_flw_rec.frqmi  != p_frqmi 
		 or w_flw_rec.strtdt != p_strtdt
		 or nvl(w_flw_rec.enddt, trunc(sysdate)) != nvl(p_enddt, trunc(sysdate)) then
		    w_chg := 'Y';
		 else
		    w_chg := 'N';
		 end if;
	     --
		 if w_chg = 'Y' then
	        declare
	          w_pm varchar2(400) := substr('Mapref='||w_flw_rec.mapref||
		                                  ' Jobflwid='||w_flw_rec.jobflwid||
		   							      ' jobschid='||w_flw_rec.jobschid, 1, 400);
            begin
	          update dwjobsch
		      set    curflg  = 'N'
		            ,recupdt = sysdate
		      where jobschid = w_flw_rec.jobschid;
	        exception
	          when others then
	            PKGERR.RAISE_ERROR(g_name, w_procnm,'107', w_pm);
            end;
	     end if;
	  end if;
	  --
	  if w_chg = 'Y' then
	     declare
	       w_pm varchar2(400) := substr('Mapref='||w_flw_rec.mapref||
	 	                               ' Jobflwid='||w_flw_rec.jobflwid||
	 	  							   ' Frqcd='||p_frqcd||
	 	  							   ' Frqdd='||p_frqdd||
	 	  							   ' Frqhh='||p_frqhh||
	 	  							   ' Frqmi='||p_frqmi, 1, 400);
         begin
	       insert into 
	 	   dwjobsch(jobschid, jobflwid, mapref, frqcd, frqdd, frqhh, frqmi
		           ,strtdt, enddt, stflg, reccrdt, recupdt, curflg, schflg)
	 	   values(DWJOBSCHSEQ.nextval, w_flw_rec.jobflwid, w_flw_rec.mapref, p_frqcd, p_frqdd, p_frqhh, p_frqmi
		         ,p_strtdt, p_enddt, 'N', sysdate, sysdate, 'Y','N')
	 	   returning jobschid into w_jobschid;
	     exception
	       when others then
	         PKGERR.RAISE_ERROR(g_name, w_procnm,'108', w_pm);
         end;
	  end if;
	  --
	  commit;
	  --
	  declare
	    w_pm varchar2(100) := substr('Mapref='||w_flw_rec.mapref,1,100);
	  begin
	    SCHEDULE_JOB(p_mapref => w_flw_rec.mapref);
	  exception
	    when others then
	      PKGERR.RAISE_ERROR(g_name, w_procnm,'145', w_pm);
      end;
	  --
	  commit;
	  --
	  return w_jobschid;
	  --
	exception
	  when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'109', w_parm);
	end CREATE_JOB_SCHEDULE;

    --
	-- Procedure to create job dependency (Hierarchy).
	--
    procedure CREATE_JOB_DEPENDENCY(p_parent_mapref  dwjobsch.mapref%type
                                   ,p_child_mapref   dwjobsch.mapref%type) is
      --
	  w_procnm  varchar2(50) := 'CREATE_JOB_DEPENDENCY';
	  --
	  w_parm    varchar2(400) := substr('ParentMapref='||p_parent_mapref||
	                                   ' ChildMapref='||p_child_mapref, 1, 400);
	  --
	  cursor sch_csr (c_mapref dwjobsch.mapref%type) is
	  select s.mapref, s.jobschid
	  from   dwjobsch s
	  where  s.curflg   = 'Y'
	  and    s.mapref   = c_mapref;
	  --
	  w_parent_rec  sch_csr%rowtype := null;
	  w_child_rec   sch_csr%rowtype := null;
	begin
	  open sch_csr(p_parent_mapref);
	  fetch sch_csr into w_parent_rec;
	  close sch_csr;
	  --
	  open sch_csr(p_child_mapref);
	  fetch sch_csr into w_child_rec;
	  close sch_csr;
	  --
	  update dwjobsch
	  set    dpnd_jobschid = w_parent_rec.jobschid
	  where  jobschid = w_child_rec.jobschid
	  and    curflg = 'Y';
	
	exception
	  when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'110', w_parm);
	end CREATE_JOB_DEPENDENCY;
	
	--
	-- Procedure to schedule job for immediate processing
	--
	procedure SCHEDULE_JOB_IMMEDIATE(p_mapref  in dwjobsch.mapref%type)
    is
	  --
	  w_procnm      varchar2(50)  := 'SCHEDULE_JOB_IMMEDIATE';
	  --
	  w_parm        varchar2(100) := substr('Mapref='||p_mapref,1,100);
	  w_job_action  varchar2(128) := null;
	  w_stat        varchar2(100) := null;
	begin
	  --
	  w_job_action := 'PKGDWPRC.PROCESS_JOB_FLOW('''||p_mapref||''');';
      --
      DBMS_SCHEDULER.CREATE_JOB (job_name   => p_mapref||'_'||to_char(sysdate,'YYYYMMDDHH24MI')
                                ,job_type   => 'PLSQL_BLOCK'
   						        ,job_action => w_job_action
 	    				        ,start_date => SYSTIMESTAMP
	  						    ,enabled    => TRUE);
      --
	  commit;
	exception
      when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'111', w_parm);
	end SCHEDULE_JOB_IMMEDIATE;
	
	--
    -- Procedure to schedule historical job for immediate processing
    --
    procedure SCHEDULE_HISTORY_JOB_IMMEDIATE(p_mapref  in dwjobsch.mapref%type
                                            ,p_strtdt  in date
								            ,p_enddt   in date
								            ,p_tlflg   in varchar2 default 'N')
    is
	  --
	  w_procnm      varchar2(50)  := 'SCHEDULE_HISTORY_JOB_IMMEDIATE';
	  --
	  w_parm        varchar2(100) := substr('Mapref='||p_mapref,1,100);
	  w_job_action  varchar2(1000):= null;
	  w_stat        varchar2(100) := null;
	begin
	  --
	  w_job_action := 'PKGDWPRC.PROCESS_HISTORICAL_DATA('''||p_mapref||''''||
	                  ',to_date('''||to_char(p_strtdt,'DD-Mon-YYYY')||''',''DD-Mon-YYYY'')'||
					  ',to_date('''||to_char(p_enddt,'DD-Mon-YYYY')||''',''DD-Mon-YYYY'')'||
					  ','''||nvl(p_tlflg,'N')||''');';
	  --
      DBMS_SCHEDULER.CREATE_JOB (job_name   => p_mapref||'_'||to_char(sysdate,'YYYYMMDDHH24MI')
                                ,job_type   => 'PLSQL_BLOCK'
   						        ,job_action => w_job_action
 	    				        ,start_date => SYSTIMESTAMP
	  						    ,enabled    => TRUE);
      --
	  commit;
	exception
      when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'155', w_parm);
	end SCHEDULE_HISTORY_JOB_IMMEDIATE;
	--                                      
	-- Private function to derive scheduler Interval
	--
	function GET_REPEAT_INTERVAL(p_frqcd  in dwjobsch.frqcd%type
	                            ,p_frqdd  in dwjobsch.frqdd%type
								,p_frqhh  in dwjobsch.frqhh%type
								,p_frqmi  in dwjobsch.frqmi%type)
    return   varchar2 is
	  --
	  w_procnm  varchar2(50)  := 'GET_REPEAT_INTERVAL';
	  --
	  w_parm    varchar2(200) := substr('Frqcd='||p_frqcd||
	                                   ' Frqdd='||p_frqdd||
									   ' FrqHH='||p_frqhh||
									   ' FrqMi='||p_frqmi,1,200);
	  --
	  w_repeat_interval  varchar2(400) := null;
	begin
      case p_frqcd
  	  when 'YR' then
  	    w_repeat_interval := 'FREQ=YEARLY; BYMONTHDAY='||p_frqdd||
  	                         '; BYHOUR='||p_frqhh||'; BYMINUTE='||p_frqmi||'; INTERVAL=1';
  	  when 'HY' then
  	    w_repeat_interval := 'FREQ=MONTHLY; BYMONTHDAY='||p_frqdd||
  	                         '; BYHOUR='||p_frqhh||'; BYMINUTE='||p_frqmi||'; INTERVAL=6';
  	  when 'MN' then
  	    w_repeat_interval := 'FREQ=MONTHLY; BYMONTHDAY='||p_frqdd||
  	                         '; BYHOUR='||p_frqhh||'; BYMINUTE='||p_frqmi||'; INTERVAL=1';
  	  when 'FN' then
  	    w_repeat_interval := 'FREQ=WEEKLY; BYDAY='||p_frqdd||
  	                         '; BYHOUR='||p_frqhh||'; BYMINUTE='||p_frqmi||'; INTERVAL=2';
  	  when 'WK' then
  	    w_repeat_interval := 'FREQ=WEEKLY; BYDAY='||p_frqdd||
  	                         '; BYHOUR='||p_frqhh||'; BYMINUTE='||p_frqmi||'; INTERVAL=1';
  	  when 'DL' then
  	    w_repeat_interval := 'FREQ=DAILY; BYHOUR='||p_frqhh||'; BYMINUTE='||p_frqmi||'; INTERVAL=1';
  	  else -- ID
  	    case
  	    when p_frqhh is not null then
  	       w_repeat_interval := 'FREQ=HOURLY; BYMINUTE='||p_frqmi||';';
  	    when p_frqhh = -1 and p_frqmi is not null then
  	       w_repeat_interval := 'FREQ=MINUTELY; INTERVAL='||p_frqmi;
  	    else
  	       null;
  	    end case;
  	  end case;
	  --
	  return w_repeat_interval;
	  --
    exception
      when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'112', w_parm);
	end GET_REPEAT_INTERVAL;	  
	  
	  
    --
	-- Private function to create Schedule chain with dependent job.
	--
	function CREATE_SCHEDULE_CHAIN(p_mapref in dwjobsch.mapref%type)
	return   varchar2 is
	  --
	  w_procnm  varchar2(50)  := 'CREATE_SCHEDULE_CHAIN';
	  --
	  w_parm    varchar2(100) := substr('Mapref='||p_mapref,1,100);
	  --
	  cursor sch_csr is
	  select s.mapref, s.jobschid, s.frqcd, s.frqdd, s.frqhh, s.frqmi
	        ,s.strtdt, s.enddt, s.dpnd_jobschid
	  from   dwjobsch s
	  where  s.mapref   = p_mapref
	  and    s.curflg   = 'Y';
	  --
	  w_sch_rec     sch_csr%rowtype := null;
	  --
	  -- Scheduler root query.
	  --
	  cursor root_csr (c_jobschid dwjobsch.jobschid%type) is
	  with schdata as (
	       select * from dwjobsch where curflg = 'Y')
	  select s1.*
	  from (select s.mapref, s.jobschid, s.frqcd, s.frqdd, s.frqhh, s.frqmi
	              ,s.strtdt, s.enddt, dpnd_jobschid
	        from   schdata s
	        connect by  jobschid = prior dpnd_jobschid
	        start with jobschid = c_jobschid) s1 
	  where s1.dpnd_jobschid is null;
	  --
	  w_root_rec    root_csr%rowtype   := null;
	  --
	  cursor chain_csr (c_chain_name varchar2) is
	  select c.chain_name, c.enabled chain_enabled
	        ,j.job_name, j.job_type, j.job_action, j.enabled job_enabled, j.repeat_interval
			,j.start_date
      from  all_scheduler_chains c, all_scheduler_jobs j
      where c.owner         = user
      and   c.chain_name    = c_chain_name
      and   j.owner(+)      = c.owner
      and   j.job_type (+)  = 'CHAIN'
      and   j.job_action(+) = c.chain_name;
	  --
	  w_chain_rec  chain_csr%rowtype := null;
	  --
	  cursor prgrm_csr (c_prgrm_name varchar2) is
	  select program_name, enabled
	  from   all_scheduler_programs 
	  where  program_name = c_prgrm_name
	  and    program_type = 'PLSQL_BLOCK';
	  --
	  w_root_prgrm_rec  prgrm_csr%rowtype := null;
	  w_prgrm_rec       prgrm_csr%rowtype := null;
	  --
	  cursor step_csr (c_mapref dwjobsch.mapref%type) is
	  select chain_name, step_name
	  from   all_scheduler_chain_steps
	  where  owner = user
	  and    chain_name = w_chain_rec.chain_name
	  and    step_name  = c_mapref;
	  --
	  w_step_rec  step_csr%rowtype := null;
	  --
	  cursor rule_csr (c_rulename varchar2) is
	  select chain_name, rule_name, action
	  from   all_scheduler_chain_rules
	  where  owner = user
	  and    chain_name = w_chain_rec.chain_name
	  and    rule_name  = c_rulename;
	  --
	  w_rule_rec  rule_csr%rowtype := null;
      --
	  w_action       varchar2(200) := 'PKGDWPRC.PROCESS_JOB_FLOW(''#'');';
	  --
	begin
	  begin
	    open  sch_csr;
	    fetch sch_csr into w_sch_rec;
	    close sch_csr;
	  exception
        when others then
	      PKGERR.RAISE_ERROR(g_name, w_procnm,'114', w_parm);
	  end;
	  --
	  declare
	    w_pm  varchar2(50) := substr('Jobschid='||w_sch_rec.jobschid,1,50);
	  begin
	    open  root_csr(w_sch_rec.jobschid);
	    fetch root_csr into w_root_rec;
	    close root_csr;
	  exception
        when others then
	      PKGERR.RAISE_ERROR(g_name, w_procnm,'115', w_pm);
	  end;
	  --
	  declare
	    w_pm  varchar2(50) := substr('Chain=CHAIN_'||w_root_rec.mapref,1,50);
	  begin
	    open  chain_csr ('CHAIN_'||w_root_rec.mapref);
	    fetch chain_csr into w_chain_rec;
	    close chain_csr;
	  exception
        when others then
	      PKGERR.RAISE_ERROR(g_name, w_procnm,'116', w_pm);
	  end;
	  --
	  declare
	    w_pm  varchar2(50) := substr('Program=Program_'||w_root_rec.mapref,1,50);
	  begin
	    open  prgrm_csr ('PROGRAM_'||w_root_rec.mapref);
		fetch prgrm_csr into w_root_prgrm_rec;
		close prgrm_csr;
	  exception
        when others then
	      PKGERR.RAISE_ERROR(g_name, w_procnm,'117', w_pm);
	  end;
	  --
	  declare
	    w_pm  varchar2(50) := substr('Program=Program_'||w_sch_rec.mapref,1,50);
	  begin
	    open  prgrm_csr ('PROGRAM_'||w_sch_rec.mapref);
	    fetch prgrm_csr into w_prgrm_rec;
	    close prgrm_csr;
	  exception
        when others then
	      PKGERR.RAISE_ERROR(g_name, w_procnm,'118', w_pm);
	  end;
	  --
	  -- Create chain if not already created.
	  --
	  if w_chain_rec.chain_name is null then
	     --
		 w_chain_rec.chain_name := 'CHAIN_'||w_root_rec.mapref;
		 --
	     declare
		   w_pm  varchar2(100) := substr('Chain='||w_chain_rec.chain_name,1,100);
		 begin
		   DBMS_SCHEDULER.CREATE_CHAIN(
		                  chain_name          => w_chain_rec.chain_name
						 ,comments            => 'Chain for '||w_sch_rec.mapref||' dependent jobs');
         exception
		   when others then
		     PKGERR.RAISE_ERROR(g_name, w_procnm,'119', w_pm);
		 end;
		 --
	  else
	     --
		 -- If chain and job already exists, disable them to make changes.
		 --
	     declare
		   w_pm  varchar2(200) := substr('Chain='||w_chain_rec.chain_name||
		                                ' Job='||w_chain_rec.job_name,1,200);
		 begin
		   DBMS_SCHEDULER.DISABLE(name  => w_chain_rec.chain_name
		                         ,force => True);
		   --
		   if w_chain_rec.job_name is not null then
		      DBMS_SCHEDULER.DISABLE(name  => w_chain_rec.job_name
		                            ,force => True);
           end if;
         exception
		   when others then
		     PKGERR.RAISE_ERROR(g_name, w_procnm,'120', w_pm);
		 end;
	  end if;
	  
	  --
	  -- Create root program if not already created.
	  --
	  if w_root_prgrm_rec.program_name is null then
	     --
		 w_root_prgrm_rec.program_name := 'PROGRAM_'||w_root_rec.mapref;
		 --
	     declare
		   w_pm  varchar2(100) := substr('Program='||w_root_prgrm_rec.program_name,1,100);
		 begin
		   DBMS_SCHEDULER.CREATE_PROGRAM(
		                  program_name        => w_root_prgrm_rec.program_name
						 ,program_type        => 'PLSQL_BLOCK'
						 ,program_action      => replace(w_action,'#',w_root_rec.mapref)
						 ,enabled             => True);
         exception
		   when others then
		     PKGERR.RAISE_ERROR(g_name, w_procnm,'121', w_pm);
		 end;
		 --
	  end if;
     
	  --
	  -- Create program if not already created.
	  --
	  if w_prgrm_rec.program_name is null then
	     --
		 w_prgrm_rec.program_name := 'PROGRAM_'||w_sch_rec.mapref;
		 --
	     declare
		   w_pm  varchar2(100) := substr('Program='||w_prgrm_rec.program_name,1,100);
		 begin
		   DBMS_SCHEDULER.CREATE_PROGRAM(
		                  program_name        => w_prgrm_rec.program_name
						 ,program_type        => 'PLSQL_BLOCK'
						 ,program_action      => replace(w_action,'#',w_sch_rec.mapref)
						 ,enabled             => True);
         exception
		   when others then
		     PKGERR.RAISE_ERROR(g_name, w_procnm,'122', w_pm);
		 end;
		 --
	  end if;
	  --
	  -- Define the chain step
	  --
	  declare
	    w_pm varchar2(200) := substr('Chain='||w_chain_rec.chain_name||
	                                ' Step='||w_root_rec.mapref,1,200);
	  begin
	    open  step_csr(w_root_rec.mapref);
		fetch step_csr into w_step_rec;
 	    close step_csr;
		--
		if w_step_rec.step_name is null then
		   declare
		     w_pm varchar2(200) := substr('Chain='||w_chain_rec.chain_name||
			                             ' Step='||w_root_rec.mapref||
										 ' Program='||w_root_prgrm_rec.program_name,1,200);
		   begin
	         DBMS_SCHEDULER.DEFINE_CHAIN_STEP(
		                    chain_name    => w_chain_rec.chain_name
		     			   ,step_name     => w_root_rec.mapref
		     			   ,program_name  => w_root_prgrm_rec.program_name);
           exception
		     when others then
		       PKGERR.RAISE_ERROR(g_name, w_procnm,'123', w_pm);
		   end;
		end if;
	  exception
	    when others then
          PKGERR.RAISE_ERROR(g_name, w_procnm,'124', w_pm);
	  end;
	  --
	  w_sch_rec := null;
	  --
	  declare
	    w_pm varchar2(200) := substr('Chain='||w_chain_rec.chain_name||
		                            ' Step='||w_sch_rec.mapref,1,200);
	  begin
	    open  step_csr(w_sch_rec.mapref);
	    fetch step_csr into w_step_rec;
	    close step_csr;
		--
		if w_step_rec.step_name is null then
		   declare
		     w_pm varchar2(200) := substr('Chain='||w_chain_rec.chain_name||
			                             ' Step='||w_sch_rec.mapref||
										 ' Program='||w_prgrm_rec.program_name,1,200);
		   begin
	         DBMS_SCHEDULER.DEFINE_CHAIN_STEP(
		                    chain_name    => w_chain_rec.chain_name
					       ,step_name     => w_sch_rec.mapref
     					   ,program_name  => w_prgrm_rec.program_name);
	       exception
		     when others then
		       PKGERR.RAISE_ERROR(g_name, w_procnm,'125', w_pm);
		   end;
		end if;
	  exception
	    when others then
             PKGERR.RAISE_ERROR(g_name, w_procnm,'126', w_pm);
	  end;
	  --
	  -- define chain RULE
	  --
	  declare
	    w_pm  varchar(200) := substr('Chain='||w_chain_rec.chain_name||
		                            ' Rule=RULE_'||w_root_rec.mapref,1,200);
	  begin
	    open  rule_csr('RULE_'||w_root_rec.mapref);
		fetch rule_csr into w_rule_rec;
		close rule_csr;
		--
		if w_rule_rec.rule_name is null then
		   begin
	         DBMS_SCHEDULER.DEFINE_CHAIN_RULE(
		                    chain_name => w_chain_rec.chain_name
			               ,rule_name  => 'RULE_'||w_root_rec.mapref
					       ,action     => 'START '||w_root_rec.mapref
					       ,condition  => 'TRUE');
           exception
		     when others then
		       PKGERR.RAISE_ERROR(g_name, w_procnm,'127', w_pm);
	       end;
	    end if;
	  exception
	    when others then
	      PKGERR.RAISE_ERROR(g_name, w_procnm,'128', w_pm);
	  end;
	  --
	  w_rule_rec := null;
	  --
	  declare
	    w_pm  varchar(200) := substr('Chain='||w_chain_rec.chain_name||
		                            ' Rule=RULE_'||w_root_rec.mapref||'_'||w_sch_rec.mapref,1,200);
	  begin
	    open  rule_csr('RULE_'||w_root_rec.mapref||'_'||w_sch_rec.mapref);
		fetch rule_csr into w_rule_rec;
		close rule_csr;
		--
        if w_rule_rec.rule_name is null then
		   begin
	         DBMS_SCHEDULER.DEFINE_CHAIN_RULE(
		                    chain_name => w_chain_rec.chain_name
					       ,rule_name  => 'RULE_'||w_root_rec.mapref||'_'||w_sch_rec.mapref
					       ,action     => 'START '||w_sch_rec.mapref
					       ,condition  => w_root_rec.mapref||' COMPLETED');
           exception
	 	     when others then
	 	       PKGERR.RAISE_ERROR(g_name, w_procnm,'129', w_pm);
	       end;
	    end if;
	  exception
		when others then
		  PKGERR.RAISE_ERROR(g_name, w_procnm,'130', w_pm);
	  end;
	  --
	  declare
	    w_pm  varchar(200) := substr('Chain='||w_chain_rec.chain_name||
		                            ' Rule=RULE_END_'||w_chain_rec.chain_name,1,200);
	  begin
	    open  rule_csr('RULE_END_'||w_chain_rec.chain_name);
		fetch rule_csr into w_rule_rec;
		close rule_csr;
		--
        if w_rule_rec.rule_name is null then
		   begin
	         DBMS_SCHEDULER.DEFINE_CHAIN_RULE(
		                    chain_name => w_chain_rec.chain_name
					       ,rule_name  => 'RULE_END_'||w_chain_rec.chain_name
					       ,action     => 'END'
					       ,condition  => w_root_rec.mapref||' COMPLETED');
           exception
	 	     when others then
	 	       PKGERR.RAISE_ERROR(g_name, w_procnm,'131', w_pm);
	       end;
	    end if;
	  exception
		when others then
		  PKGERR.RAISE_ERROR(g_name, w_procnm,'132', w_pm);
	  end;
	  --
	  -- Enable Chain
	  --
	  declare
	    w_pm varchar2(100) := substr('Chain='||w_chain_rec.chain_name,1,100);
	  begin
	    DBMS_SCHEDULER.ENABLE(name => w_chain_rec.chain_name);
      exception
		when others then
		  PKGERR.RAISE_ERROR(g_name, w_procnm,'133', w_pm);
	  end;
	  --
	  declare
	    w_pm varchar2(200) := substr('Frqcd='||w_root_rec.frqcd||
	                                ' Frqdd='||w_root_rec.frqdd||
									' FrqHH='||w_root_rec.frqhh||
									' FrqMi='||w_root_rec.frqmi,1,200);
		--
	    w_repeat_interval varchar2(400) := null;
	  begin
	    w_repeat_interval := GET_REPEAT_INTERVAL(w_root_rec.frqcd
	 	                                        ,w_root_rec.frqdd
	 									        ,w_root_rec.frqhh
	 	  										,w_root_rec.frqmi);
	    --
	    -- Create job
	    --
	    if w_chain_rec.job_name is not null then
	       if w_chain_rec.repeat_interval != w_repeat_interval 
	       or to_char(w_chain_rec.start_date,'YYYYMMDDHH24MI') != to_char(w_root_rec.strtdt,'YYYYMMDDHH24MI') then
	 	      declare
			    w_pm varchar2(100) := substr('Chain='||w_chain_rec.chain_name,1,100);
	 	 	  begin
	 	 	    DBMS_SCHEDULER.DROP_JOB(
	 	 	                   job_name => 'JOB_'||w_chain_rec.chain_name
	 	 					  ,force    => True
	 	 					  ,defer    => True);
              exception
		        when others then
		          PKGERR.RAISE_ERROR(g_name, w_procnm,'134', w_pm);
	 	 	  end;
	 	   end if;
	    else
		   declare
		     w_pm varchar2(400) := substr('Chain='||w_chain_rec.chain_name||
			                             ' Interval='||w_repeat_interval,1,400);
           begin
	 	     DBMS_SCHEDULER.CREATE_JOB (
                            job_name        => 'JOB_'||w_chain_rec.chain_name
	 	  			       ,job_type        => 'CHAIN'
	 	  			       ,job_action      => w_chain_rec.chain_name
	 	  			       ,repeat_interval => w_repeat_interval
	   	     		       ,start_date      => w_root_rec.strtdt
	 	  			       ,end_date        => w_root_rec.enddt
	 	  			       ,enabled         => False );
           exception
	 	     when others then
	 	       PKGERR.RAISE_ERROR(g_name, w_procnm,'135', w_pm);
	       end;
		end if;
		--
	  exception
	 	when others then
	 	  PKGERR.RAISE_ERROR(g_name, w_procnm,'136', w_pm);
	  end;
      --
	  return w_chain_rec.chain_name;
	  --
	exception
	  when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'137', w_parm);
	end CREATE_SCHEDULE_CHAIN;
	
	--
    -- Procedure to schedule a job
    --
    procedure SCHEDULE_JOB(p_mapref in dwjobflw.mapref%type) is
	  --
	  w_procnm  varchar2(50) := 'SCHEDULE_JOB';
	  --
	  w_parm    varchar2(400) := substr('Mapref='||p_mapref,1,400);
	  --
	  cursor sch_csr is
	  select s.mapref, s.jobschid, s.frqcd, s.frqdd, s.frqhh, s.frqmi
	        ,s.strtdt, s.enddt, s.dpnd_jobschid, count(d.jobschid) cnt
	  from   dwjobsch s
	        ,dwjobsch d
	  where  s.mapref   = p_mapref
	  and    s.curflg   = 'Y'
	  and    d.dpnd_jobschid (+) = s.jobschid
	  and    d.curflg (+) = 'Y'
	  group by s.mapref, s.jobschid, s.frqcd, s.frqdd, s.frqhh, s.frqmi
	          ,s.strtdt, s.enddt, s.dpnd_jobschid;
	  --
	  w_sch_rec         sch_csr%rowtype := null;
	  --
	  w_job_name        varchar2(128)   := 'JOB_'||p_mapref;
	  --
	  cursor job_csr is
	  select j.job_name, j.enabled job_enabled
      from  all_scheduler_jobs j
      where j.owner    = user
      and   j.job_name = w_job_name;
	  --
	  w_job_rec  job_csr%rowtype := null;
	  --
	  w_job_action      varchar2(200) := 'PKGDWPRC.PROCESS_JOB_FLOW('''||p_mapref||''');';
	  w_repeat_interval varchar2(400) := null;
	  w_chain           varchar2(50)  := null;
	  --
    begin
	  begin
	    open  sch_csr;
	    fetch sch_csr into w_sch_rec;
	    close sch_csr;
	  exception
        when others then
	      PKGERR.RAISE_ERROR(g_name, w_procnm,'138', w_parm);
	  end;
	  --
	  declare
	    w_pm varchar2(400) := substr('FrqCD='||w_sch_rec.frqcd||
		                            ' FrqDD='||w_sch_rec.frqdd||
									' FrqHH='||w_sch_rec.frqhh||
									' FrqMI='||w_sch_rec.frqmi,1,400);
	  begin
	    w_repeat_interval := PKGDWPRC.GET_REPEAT_INTERVAL(w_sch_rec.frqcd
		                                                 ,w_sch_rec.frqdd
														 ,w_sch_rec.frqhh
														 ,w_sch_rec.frqmi);
      exception
        when others then
	      PKGERR.RAISE_ERROR(g_name, w_procnm,'139', w_pm);
	  end;
	  --
	  if nvl(w_sch_rec.cnt,0) > 0 or w_sch_rec.dpnd_jobschid is not null then
	     declare
	       w_pm varchar2(200) := substr('Mapref='||p_mapref||
		                               ' JObSchId='||w_sch_rec.jobschid,1, 200);
	     begin
	       w_chain := PKGDWPRC.CREATE_SCHEDULE_CHAIN(p_mapref);
	     exception
           when others then
	         PKGERR.RAISE_ERROR(g_name, w_procnm,'140', w_parm);
	     end;
	  else
	    --
	    if w_sch_rec.jobschid is not null then
		   begin
	         open  job_csr;
	         fetch job_csr into w_job_rec;
	         close job_csr;
	       exception
             when others then
	           PKGERR.RAISE_ERROR(g_name, w_procnm,'152', w_parm);
	       end;
	       --
		   if w_job_rec.job_name is not null then
		      declare
			    w_pm varchar2(100) := substr(w_job_rec.job_name,1,100);
	 	 	  begin
	 	 	    DBMS_SCHEDULER.DROP_JOB(
	 	 	                   job_name => w_job_rec.job_name
	 	 					  ,force    => True
	 	 					  ,defer    => True);
              exception
		        when others then
		          PKGERR.RAISE_ERROR(g_name, w_procnm,'153', w_pm);
	 	 	  end;
		   end if;
		   --
	       declare
	         w_pm  varchar2(400) := substr('Jobname='||w_job_name||
	 	                                  ' Action='||w_job_action||
	 	    							  ' Interval='||w_repeat_interval,1,400);
	       begin
             DBMS_SCHEDULER.CREATE_JOB (job_name        => w_job_name
	 	                               ,job_type        => 'PLSQL_BLOCK'
	 	    						   ,job_action      => w_job_action
	 	    						   ,repeat_interval => w_repeat_interval
	 	    						   ,start_date      => w_sch_rec.strtdt
		  							   ,end_date        => w_sch_rec.enddt
	 	    						   ,enabled         => FALSE);
	       exception
             when others then
	           PKGERR.RAISE_ERROR(g_name, w_procnm,'141', w_pm);
	       end;
		   --
		   
	    end if;
	    --
	  end if;
	  --
	exception
	  when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'142', w_parm);
	end SCHEDULE_JOB;
    --
    -- Procedure to schedule all jobs.
    --
    procedure SCHEDULE_ALL_JOBS is
	  --
	  w_procnm  varchar2(50) := 'SCHEDULE_ALL_JOBS';
	  --
	  w_parm    varchar2(400) := null;
	  --
	  cursor sch_csr is
	  select distinct s.mapref
	  from   dwjobsch s
	  where  s.curflg   = 'Y';
	begin
	  for sch_rec in sch_csr loop
	    declare
		  w_pm varchar2(400) := substr('Mapref='||sch_rec.mapref, 1, 400);
		begin
		  PKGDWPRC.SCHEDULE_JOB(p_mapref => sch_rec.mapref);
		exception
	      when others then
	        PKGERR.RAISE_ERROR(g_name, w_procnm,'143', w_pm);  
		end;
		--
	  end loop;
	  
	exception
	  when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'144', w_parm);
	end SCHEDULE_ALL_JOBS;
	
	--
    -- Procedure to enable or disable a job scheduler.
    --
	procedure ENABLE_DISABLE_SCHEDULE(p_mapref in dwjobsch.mapref%type
	                                 ,p_action in varchar2) is
      --
	  w_procnm  varchar2(50) := 'ENABLE_DISABLE_SCHEDULE';
	  --
	  w_parm    varchar2(400) := null;
	  --
	  cursor job_csr is
	  select job_name, enabled
	  from   user_scheduler_jobs
	  where  job_name = 'JOB_'||p_mapref;
	  --
	  w_job_rec job_csr%rowtype := null;
	  w_schflg  varchar2(1)     := null;
	  --
	  w_msg     varchar2(200) := null;
	begin
	  case 
	  when p_mapref is null then
	     w_msg := 'Mapping reference not provided.';
	  when nvl(p_action,'#') not in ('E','D') then
	     w_msg := 'Action must be provided, valid values are E or D.';
	  else
	     w_msg := null;
	  end case;
	  --
	  if w_msg is not null then
	     w_parm := w_parm||'-'||w_msg;
		 Raise value_error;
	  end if;
	  --
	  open  job_csr;
	  fetch job_csr into w_job_rec;
	  close job_csr;
	  --
	  case
	  when upper(w_job_rec.enabled) = 'FALSE'
	  and  p_action = 'E' then
	       declare
		     w_pm varchar2(100) := substr('Jobname='||w_job_rec.job_name,1,100);
		   begin
		     DBMS_SCHEDULER.ENABLE(w_job_rec.job_name);
			 w_schflg := 'Y';
	       exception
	         when others then
	           PKGERR.RAISE_ERROR(g_name, w_procnm,'145', w_pm);
	       end;
	  when upper(w_job_rec.enabled) = 'TRUE'
	  and  p_action = 'D' then
	       declare
		     w_pm varchar2(100) := substr('Jobname='||w_job_rec.job_name,1,100);
		   begin
		     DBMS_SCHEDULER.DISABLE(w_job_rec.job_name);
		 	 w_schflg := 'N';
	       exception
	         when others then
	           PKGERR.RAISE_ERROR(g_name, w_procnm,'146', w_pm);
	       end;
	  else
	     -- no change required.
		 null;
	  end case;
	  --
	  if w_schflg is not null then
	     begin
	 	   update dwjobsch
	 	   set    schflg = w_schflg
	 	         ,recupdt = sysdate
	 	   where mapref = p_mapref
	 	   and   curflg = 'Y'
	 	   and   nvl(schflg,'#') != w_schflg;
         exception
	       when others then
	         PKGERR.RAISE_ERROR(g_name, w_procnm,'147', w_parm);
	     end;
	  end if;
	  --
	  commit;
	  --
	exception
	  when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'148', w_parm);
	end ENABLE_DISABLE_SCHEDULE;
    --	
	
	--
    -- Procedure to STOP a running job
    --
    procedure STOP_RUNNING_JOB(p_mapref in  dwprclog.mapref%type
                              ,p_strtdt in  dwprclog.strtdt%type
							  ,p_force  in  varchar2 default 'Y'
							  ,p_err    out varchar2) is
      --
	  w_procnm  varchar2(50) := 'STOP_RUNNING_JOB';
	  --
	  w_parm    varchar2(400) := substr('Mapref='||p_mapref||
	                                   ' StartDt='||to_char(p_strtdt,'DD-Mon-YYYY')||
									   ' Force='||p_force, 1, 400);
	  --
	  cursor csr1 is
	  select job_name
	  from   user_scheduler_jobs
	  where  state = 'RUNNING'
	  and    instr(job_name,p_mapref) > 0
      and    trunc(start_date) = trunc(p_strtdt);
	  --
	  w_job_name varchar2(120) := null;
    begin
	  open csr1;
	  fetch csr1 into w_job_name;
	  close csr1;
	  --
	  if w_job_name is not null then
	     begin
	       DBMS_SCHEDULER.STOP_JOB(job_name => w_job_name
                                  ,force    => case p_force when 'Y' then True else False end);
         exception 
		   when others then
		     p_err := 'Job Name ='||w_job_name||' Error:'||sqlerrm;
	     end;
		 --	 
         begin
           update dwprclog
           set    status = 'ST'
           where  mapref = p_mapref
           and    status = 'IP'
           and    trunc(strtdt) = trunc(p_strtdt);
	     exception
	       when others then
	         PKGERR.RAISE_ERROR(g_name, w_procnm,'157', w_parm);
         end;	
	     --
		 commit;
      end if;
	  --
    exception
	  when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'156', w_parm);
	end STOP_RUNNING_JOB;
	
	
end PKGDWPRC;
/
