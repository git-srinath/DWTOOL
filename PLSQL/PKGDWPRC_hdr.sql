--
-- Package for processing jobs.
--
/*
Change history:
date        who              Remarks
----------- ---------------- ------------------------------------------------------------------
28-May-2025 Srinath C V      PROCESS_JOB_FLOW updated to use 5 DWT parameters.
06-Jun-2025 Srinath C V      PROCESS_JOB_FLOW updates to add 5 more DWT parameters.
							 CREATE_JOB_LOG amended to add PRCID parameter.
18-Jun-2025 Srinath C V      Added LOG_JOB_ERROR procedure.
01-Jul-2025 Srinath C V      STOP_RUNNING_JOB added.

*/
create or replace package PKGDWPRC is
  --
  -- Function to get this package version.
  --
  function VERSION return varchar;
  
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
						  ,p_errrows   in     dwjoblog.errrows%type);
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
  return dwjobsch.jobschid%type;
  --
  -- Procedure to create job dependency
  --
  procedure CREATE_JOB_DEPENDENCY(p_parent_mapref  dwjobsch.mapref%type
                                 ,p_child_mapref   dwjobsch.mapref%type);
  --
  -- Procedure to process job flow.
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
							,p_param10 in varchar2 default null	);
							
  --
  -- Procedure to process historical data, created for BCCB DW project.
  --
  procedure PROCESS_HISTORICAL_DATA(p_mapref in dwjobflw.mapref%type
                                   ,p_strtdt in date
								   ,p_enddt  in date
	                               ,p_tl_flg in varchar2);
  
  --
  -- Procedure to schedule job for immediate processing
  --
  procedure SCHEDULE_JOB_IMMEDIATE(p_mapref  in dwjobsch.mapref%type);
  
  --
  -- Procedure to schedule historical job for immediate processing
  --
  procedure SCHEDULE_HISTORY_JOB_IMMEDIATE(p_mapref  in dwjobsch.mapref%type
                                          ,p_strtdt  in date
								          ,p_enddt   in date
								          ,p_tlflg   in varchar2 default 'N');
	
  --
  -- Procedure to schedule a job with intervals
  --
  procedure SCHEDULE_JOB(p_mapref in dwjobflw.mapref%type);
  
  --
  -- Procedure to schedule all jobs as per intervals defined.
  --
  procedure SCHEDULE_ALL_JOBS;
  
  --
  -- Procedure to enable or disable a job scheduler.
  --
  procedure ENABLE_DISABLE_SCHEDULE(p_mapref in dwjobsch.mapref%type
	                               ,p_action in varchar2);
									 
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
						 ,p_keyvalue  in dwjoberr.keyvalue%type);
						 
  --
  -- Procedure to STOP a running job
  --
  procedure STOP_RUNNING_JOB(p_mapref in dwprclog.mapref%type
                            ,p_strtdt in dwprclog.strtdt%type
							,p_force  in varchar2 default 'Y'
						    ,p_err    out varchar2);

end PKGDWPRC;
/
