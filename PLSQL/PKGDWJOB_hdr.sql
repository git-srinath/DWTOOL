--
-- Package for validating and processing mappings provided.
--
create or replace package PKGDWJOB authid current_user is
  --
  -- Function to get this package version.
  --
  function version return varchar;
  --
  -- Function to create or update Jobs.
  -- Mapping logic must be verfied before creating job details.
  --
  function CREATE_UPDATE_JOB(p_mapref in dwmapr.mapref%type) 
  return   dwjob.jobid%type;
  --
  -- Function to create target tables as per mapping.
  -- returns status (Y-Sucess, N-Not sucess).
  --
  function CREATE_TARGET_TABLE(p_mapref in dwmapr.mapref%type)
  return   varchar2;
  --
  -- Procedure to create jobs for all the mappings.
  --
  Procedure CREATE_ALL_JOBS;

  -- 
  -- procedure to create job flow record.
  -- Job flow is a PLSQL block dynamically created basis the mappings creates.
  -- The PLSQL block is stored in a clob column in database.
  --
  procedure CREATE_JOB_FLOW (p_mapref  in dwmapr.mapref%type);

end PKGDWJOB;
/
