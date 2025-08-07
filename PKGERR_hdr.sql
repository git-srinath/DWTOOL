--
-- Package for error processing.
--

create or replace package PKGERR is

  procedure raise_error(
     p_mod   in varchar
    ,p_nam   in varchar
    ,p_blk   in varchar
    ,p_msg   in varchar);
	  
end PKGERR;
/
