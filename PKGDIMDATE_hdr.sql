--
-- Package for processing dim date records.
--

create or replace package PKGDIMDATE is
  --
  -- Function to get this package version.
  --
  function VERSION return varchar;
  
  --
  -- Procedure to load Date dimension records.
  --
  procedure LOAD_DIM_DATE(p_startdt  in Date
                         ,p_enddt    in date);

end PKGDIMDATE;
/
