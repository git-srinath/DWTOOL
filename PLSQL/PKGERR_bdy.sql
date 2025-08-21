--
-- Package for error processing.
--
create or replace package body PKGERR is

  g_name constant varchar2(10) := 'PKGERR';
  g_ver  constant varchar2(10) := 'V001';
  
  function version return varchar is
  begin
    return g_name||';'||g_ver;
  end;
  
  --
  -- Procedure for error handling
  --
  procedure raise_error(
     p_mod   in varchar
    ,p_nam   in varchar
    ,p_blk   in varchar
    ,p_msg   in varchar) is
   --
   w_err exception;
   w_msg varchar2(4000) := g_name||':'||g_ver;
  begin 
    w_msg := p_mod || ' - '||
  		     p_nam || ':-'||chr(10)||
  		     p_blk ||' : '||
  		     p_msg|| chr(10)||' Err:'||sqlerrm;
    raise w_err;
  exception 
    when others then 
  	raise_application_error(-20099, w_msg);
  end raise_error;

end PKGERR;
/