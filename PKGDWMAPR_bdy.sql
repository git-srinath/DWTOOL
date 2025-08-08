--
-- Package for validating and processing mappings provided.
--
/*
Last error: 141
Change history:
date        who              Remarks
----------- ---------------- ----------------------------------------------------------------------------------------
28-May-2025 Srinath C V      VALIDATE_MAPPING_DETAILS updated to check for primary key entries.
							 VALIDATE_LOGIC function amended to evaluate logic by replacing parameters to null.
30-May-2025 Srinath C V      CREATE_UPDATE_MAPPING_DETAIL amended to check data type specified in the mappings.
21-Jun-2025 Srinath C V      CREATE_UPDATE_MAPPING amended to allow null values to FRQCD.
25-Jun-2025 Srinath C V      Amended VALIDATE_LOGIC to validate logic if not already validated.
25-Jun-2025 Srinath C V		 Change, reverted.
28-Jun-2025 Srinath C V      Intro for SQL logic capture, and mapref increase.
02-Jul-2025 Srinath C V      Bug fix applied to VALIDATE_LOGIC.

*/
create or replace package body PKGDWMAPR is 

	g_name constant varchar2(10) := 'PKGDWMAPR';
	g_ver  constant varchar2(10) := 'V001';
	--
	g_user dwmapr.crtdby%type    := null;

	function version return varchar is
	begin
	  return g_name||':'||g_ver;
	end;
	
	--
    -- Function to record SQL query
    --
    function CREATE_UPDATE_SQL(p_dwmaprsqlcd in dwmaprsql.dwmaprsqlcd%type
                              ,p_dwmaprsql   in dwmaprsql.dwmaprsql%type)
    return dwmaprsql.dwmaprsqlid%type is
	  --
	  w_procnm  varchar2(50) := 'CREATE_UPDATE_SQL';
	  --
	  w_parm    varchar2(100) := substr('SqlCode='||p_dwmaprsqlcd,1,100);
	  --
	  cursor    msql_cur is 
	  select dwmaprsqlid, dwmaprsqlcd, dwmaprsql
	  from  dwmaprsql 
	  where dwmaprsqlcd = p_dwmaprsqlcd
	  and   curflg = 'Y';
	  --
	  w_rec     msql_cur%rowtype;
	  --
	  w_chg     varchar2(1) := 'Y';
	  w_res     integer;
	  --
	  w_msg     varchar2(200) := null;
	  --
	  w_return  dwmaprsql.dwmaprsqlid%type;
	  --
	begin
	  if nvl(p_dwmaprsqlcd,'') is null then
	     w_msg := 'The mapping SQL Code cannot be null.';
	  end if;
	  --
	  if instr(p_dwmaprsqlcd,' ') > 0 then
	     w_msg := 'Space(s) not allowed to form mapping SQL Code.';
	  end if;
      --
	  if  w_msg is null
	  and nvl(DBMS_LOB.GETLENGTH(p_dwmaprsql),0) = 0 then
	      w_msg := 'The SQL Query cannot be blank.';
	  end if;
	  --
	  if w_msg is not null then
	     w_parm := w_parm||'::'||w_msg;
		 raise value_error;
	  end if;
	  --
	  open msql_cur;
	  fetch msql_cur into w_rec;
	  close msql_cur;
	  --
	  if w_rec.dwmaprsqlcd is not null then
	     begin
	       w_res := DBMS_LOB.COMPARE(p_dwmaprsql, w_rec.dwmaprsql);
	     exception
	       when others then
	 	    PKGERR.RAISE_ERROR(g_name, w_procnm,'131', w_parm);
         end;
		 --
		 w_return := w_rec.dwmaprsqlid;
	  else 
	     w_res := 1;
	  end if;

	  if w_res != 0 then
	     if w_rec.dwmaprsqlcd is not null then
	        begin
			  update dwmaprsql
			  set curflg  = 'N'
			     ,recupdt = sysdate
			  where dwmaprsqlcd = w_rec.dwmaprsqlcd
			  and   curflg = 'Y';
			exception
	          when others then
	 	        PKGERR.RAISE_ERROR(g_name, w_procnm,'132', w_parm);
            end;
		 end if;
		--
		begin
		  insert into 
		  dwmaprsql (dwmaprsqlid, dwmaprsqlcd, dwmaprsql, reccrdt, recupdt, curflg)
		  values (DWMAPRSQLSEQ.nextval, p_dwmaprsqlcd, regexp_replace(p_dwmaprsql,';',''), sysdate, sysdate, 'Y')
		  returning dwmaprsqlid into w_return;
		exception
	      when others then
	 	    PKGERR.RAISE_ERROR(g_name, w_procnm,'133', w_parm);
        end;
	  end if;
      --
      return w_return;
      --	  
	exception
	  when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'134', w_parm);
    end CREATE_UPDATE_SQL;
  
	--
	-- Function body to create or update mappings, returns mapping ID
	-- Any change is to be historised.
	--
	function CREATE_UPDATE_MAPPING(
	   p_mapref     in dwmapr.mapref%type
	  ,p_mapdesc    in dwmapr.mapdesc%type
	  ,p_trgschm    in dwmapr.trgschm%type
	  ,p_trgtbtyp   in dwmapr.trgtbtyp%type
	  ,p_trgtbnm    in dwmapr.trgtbnm%type
	  ,p_frqcd      in dwmapr.frqcd%type
	  ,p_srcsystm   in dwmapr.srcsystm%type
	  ,p_lgvrfyflg  in dwmapr.lgvrfyflg%type
	  ,p_lgvrfydt   in dwmapr.lgvrfydt%type
	  ,p_stflg      in dwmapr.stflg%type
	  ,p_blkprcrows in dwmapr.blkprcrows%type)
	return dwmapr.mapid%type is
	  --
	  w_procnm  varchar2(50) := 'CREATE_UPDATE_MAPPING';
	  --
	  w_parm    varchar2(200) := substr('Mapref='||p_mapref||'-'||p_mapdesc,1,200);
	  --
	  cursor    mapr_cur (c_mapref dwmapr.mapref%type) is 
	  select * 
	  from dwmapr 
	  where mapref = c_mapref
	  and   curflg = 'Y';
	  --
	  w_mapr_rec mapr_cur%rowtype;
	  w_chg      varchar2(1) := 'Y';
	  --
	  w_mapid    dwmapr.mapid%type := null;
	  --
	  w_msg      varchar2(200) := null;
	  --
	begin
	  case
	  when nvl(p_mapref,'') = '' then
	    w_msg := 'Mapping reference not provided.';
	  when nvl(p_trgtbtyp,'X') not in ('NRM','DIM','FCT','MRT') then
	    w_msg := 'Invalid target table type (valid: NRM,DIM,FCT,MRT).';
      when nvl(p_frqcd,'NA') not in ('NA','ID','DL','WK','FN','MN','HY','YR') then
	    w_msg := 'Invalid frequency code (Valid: ID,DL,WK,FN,MN,HY,YR).';
      when nvl(p_stflg,'N') not in ('A','N') then
	    w_msg := 'Invalid status (Valid: A,N).';
      when nvl(p_lgvrfyflg,'N') not in ('Y','N') then
	    w_msg := 'Invalid verification flag (Valid: Y,N).';
	  when p_srcsystm is null then
	    w_msg := 'Source system not provided.';
		--
	  when p_trgschm is null then
	    w_msg := 'Target Schema name not provided.';
	  when instr(p_trgschm,' ') > 0 then
	    w_msg := 'Traget schema name must not contain blank spaces';
	  when regexp_like(p_trgschm,'[^A-Za-z0-9_]') then
	    w_msg := 'Special characters not allowed to form target schema name.';
	  when regexp_like(substr(p_trgschm,1,1),'^\d') then 
	    w_msg := 'Target schema name must not start with number.';
		--
	  when instr(p_trgtbnm,' ') > 0 then
	    w_msg := 'Traget table name must not contain blank spaces';
	  when regexp_like(p_trgtbnm,'[^A-Za-z0-9_]') then
	    w_msg := 'Special characters not allowed to form target table name.';
	  when regexp_like(substr(p_trgtbnm,1,1),'^\d') then 
	    w_msg := 'Target table must not start with number.';
	    --
	  when (p_lgvrfyflg is not null and p_lgvrfydt is null)
      or   (p_lgvrfyflg is null and p_lgvrfydt is not null)	then
	    w_msg := 'Both logic verification flag and date must be provide or both must be blank.';
	    --
	  when nvl(p_blkprcrows,0) < 0 then
	    w_msg := 'The number of Bulk Processing Rows cannot be negative.';
	  else
	    w_msg := null;
	  end case;
	  --
	  if w_msg is not null then
	     w_parm := w_parm||'::'||w_msg;
		 raise value_error;
	  end if;
	  --
	  open mapr_cur(p_mapref);
	  fetch mapr_cur into w_mapr_rec;
	  close mapr_cur;
	  --
	  if w_mapr_rec.mapref is not null then
		 --
		 if w_mapr_rec.mapdesc           != p_mapdesc
		 or w_mapr_rec.trgschm           != p_trgschm  
		 or w_mapr_rec.trgtbtyp          != p_trgtbtyp
		 or w_mapr_rec.trgtbnm           != p_trgtbnm  
		 or w_mapr_rec.frqcd             != p_frqcd        
		 or w_mapr_rec.srcsystm          != p_srcsystm 
		 or w_mapr_rec.lgvrfyflg         != nvl(p_lgvrfyflg,'N')
		 or w_mapr_rec.lgvrfydt          != p_lgvrfydt 
		 or w_mapr_rec.stflg             != nvl(p_stflg,'N')  
		 or nvl(w_mapr_rec.blkprcrows,0) != nvl(p_blkprcrows,0) then
			-- New record has changes, update required.
			w_chg := 'Y';
		 else
			-- existing record is same as new record no change or insert required.
			-- return the existing id.
			w_chg := 'N';
			w_mapid := w_mapr_rec.mapid;
		 end if;
		 --
		 if w_chg = 'Y' then
			declare
			  w_pm varchar2(200) := w_parm||' mapid='||to_char(w_mapr_rec.mapid);
			begin
			  update dwmapr
			  set    curflg  = 'N'
			        ,recupdt = sysdate
					,uptdby  = g_user
			  where  mapid = w_mapr_rec.mapid;
			exception
			  when others then
				PKGERR.RAISE_ERROR(g_name, w_procnm,'101', w_pm);
			end;
		 end if;
	  end if;
	  --
	  -- Insert new record.
	  --
	  if w_chg = 'Y' then
		 begin
		   insert into 
		   dwmapr (mapid, mapref, mapdesc, trgschm, trgtbtyp, trgtbnm, frqcd, srcsystm
				  ,lgvrfyflg, lgvrfydt, stflg, reccrdt, recupdt, curflg, blkprcrows, crtdby, uptdby)
		   values (dwmaprseq.nextval ,p_mapref, p_mapdesc, p_trgschm, p_trgtbtyp, p_trgtbnm, p_frqcd, p_srcsystm
				  ,nvl(p_lgvrfyflg,'N'), p_lgvrfydt, nvl(p_stflg,'N'), sysdate, sysdate, 'Y', p_blkprcrows, g_user, g_user)
		   returning mapid into w_mapid;
		 exception
		   when others then
				PKGERR.RAISE_ERROR(g_name, w_procnm,'102', w_parm);
		 end;
		 --
		 
	  end if;
	  --
	  return w_mapid;
	  --
	exception 
	  when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'103', w_parm);
	end CREATE_UPDATE_MAPPING;
	
	--
	-- Function body to create or update mappings, returns mapping ID
	-- Any change is to be historised.
	--
	function CREATE_UPDATE_MAPPING(
	   p_mapref     in dwmapr.mapref%type
	  ,p_mapdesc    in dwmapr.mapdesc%type
	  ,p_trgschm    in dwmapr.trgschm%type
	  ,p_trgtbtyp   in dwmapr.trgtbtyp%type
	  ,p_trgtbnm    in dwmapr.trgtbnm%type
	  ,p_frqcd      in dwmapr.frqcd%type
	  ,p_srcsystm   in dwmapr.srcsystm%type
	  ,p_lgvrfyflg  in dwmapr.lgvrfyflg%type
	  ,p_lgvrfydt   in dwmapr.lgvrfydt%type
	  ,p_stflg      in dwmapr.stflg%type
	  ,p_blkprcrows in dwmapr.blkprcrows%type
	  ,p_user       in dwmapr.crtdby%type)
	return dwmapr.mapid%type is
	  --
	  w_procnm  varchar2(50)      := 'CREATE_UPDATE_MAPPING';
	  --
	  w_parm    varchar2(400)     := substr('Mapref='||p_mapref||'-'||p_mapdesc||
	                                       ' User='||p_user,1,400);
	  w_mapid   dwmapr.mapid%type := null;
	begin
	  if p_user is null then
	     w_parm := w_parm||'::'||'Session user not provided.';
		 raise value_error;
	  end if;
	  --
	  g_user := p_user;
	  --
	  w_mapid := PKGDWMAPR.CREATE_UPDATE_MAPPING(p_mapref
	                                            ,p_mapdesc   
	                                            ,p_trgschm   
	                                            ,p_trgtbtyp  
	                                            ,p_trgtbnm   
	                                            ,p_frqcd     
	                                            ,p_srcsystm  
	                                            ,p_lgvrfyflg 
	                                            ,p_lgvrfydt  
	                                            ,p_stflg     
	                                            ,p_blkprcrows);
      --
	  return w_mapid;
	  --
	exception 
	  when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'104', w_parm);
	end CREATE_UPDATE_MAPPING;
	
	--
	-- Function body to create or update mappings, returns mapping detail ID
	-- Any change is to be historised.
	--
	function CREATE_UPDATE_MAPPING_DETAIL(
       p_mapref     in dwmaprdtl.mapref%type
      ,p_trgclnm    in dwmaprdtl.trgclnm%type
      ,p_trgcldtyp  in dwmaprdtl.trgcldtyp%type
      ,p_trgkeyflg  in dwmaprdtl.trgkeyflg%type
      ,p_trgkeyseq  in dwmaprdtl.trgkeyseq%type
      ,p_trgcldesc  in dwmaprdtl.trgcldesc%type
      ,p_maplogic   in dwmaprdtl.maplogic%type
	  ,p_keyclnm    in dwmaprdtl.keyclnm%type
	  ,p_valclnm    in dwmaprdtl.valclnm%type
      ,p_mapcmbcd   in dwmaprdtl.mapcmbcd%type
      ,p_excseq     in dwmaprdtl.excseq%type
      ,p_scdtyp     in dwmaprdtl.scdtyp%type
      ,p_lgvrfyflg  in dwmaprdtl.lgvrfyflg%type
      ,p_lgvrfydt   in dwmaprdtl.lgvrfydt%type )  
    return dwmaprdtl.mapdtlid%type is
	  --
	  w_procnm  varchar2(50) := 'CREATE_UPDATE_MAPPING_DETAIL';
	  --
	  w_parm    varchar2(400) := substr('Mapref='||p_mapref||
	                                   ' Trgcol='||p_trgclnm,1,400);
	  --
	  cursor msql_cur (c_sqlcd  dwmaprsql.dwmaprsqlcd%type) is
	  select dwmaprsqlid, dwmaprsqlcd
	  from   dwmaprsql
	  where  dwmaprsqlcd = c_sqlcd
	  and    curflg = 'Y';
	  --
	  w_msql_rec  msql_cur%rowtype := null;
	  --
	  cursor mapr_cur (c_mapref dwmapr.mapref%type) is 
	  select * 
	  from dwmapr 
	  where mapref = c_mapref
	  and   curflg = 'Y';
	  --
	  w_mapr_rec mapr_cur%rowtype := null;
	  --
	  cursor maprdtl_cur (c_mapref  dwmaprdtl.mapref%type
	                     ,c_trgclnm dwmaprdtl.trgclnm%type) is 
	  select * 
	  from dwmaprdtl d
	  where mapref = c_mapref
	  and   trgclnm = c_trgclnm
	  and   curflg = 'Y';
	  --
	  w_maprdtl_rec maprdtl_cur%rowtype := null;
	  --
	  cursor dtyp_cur (c_prcd dwparams.prcd%type) is
	  select prval
	  from   dwparams
	  where  prtyp = 'Datatype'
	  and    prcd  = c_prcd;
	  --
	  w_dtyp_rec dtyp_cur%rowtype := null;
	  --
	  w_chg      varchar2(1)   := 'Y';
	  w_msg      varchar2(200) := null;
	  --
	  w_mapdtlid dwmaprdtl.mapdtlid%type := null;
	  --
	begin
	  case
	  when p_mapref is null then
	    w_msg := 'Mapping reference not provided.';
		--
	  when p_trgclnm is null then
	    w_msg := 'Target column name not provided.';
	  when instr(p_trgclnm,' ') > 0 then
	    w_msg := 'Traget column name must not contain blank spaces';
	  when regexp_like(p_trgclnm,'[^A-Za-z0-9_]') then
	    w_msg := 'Special characters not allowed to form target column name.';
	  when regexp_like(substr(p_trgclnm,1,1),'^\d') then 
	    w_msg := 'Target column name must not start with number.';
		--
	  when p_trgcldtyp is null then
	    w_msg := 'Target column data type is not provided.';
		--
      when nvl(p_trgkeyflg,'N') not in ('Y','N') then
	    w_msg := 'Invalid value for Key flag (valid: Y or blank).';
	  when p_trgkeyflg = 'Y' and p_trgkeyseq is null then
	    w_msg := 'Key sequence must be provided for Primary key columns.';
      when p_maplogic is null  then
	    w_msg := 'Mapping logic must be provided.';
	  when (p_maplogic is not null and (p_keyclnm is null or p_valclnm is null)) then 
	    w_msg := 'Key column and value column must be provided.';
	  when nvl(p_scdtyp,1) not in (1,2,3) then
	    w_msg := 'Invalid values for SCD type.';  
	  when (p_lgvrfyflg is not null and p_lgvrfydt is null)
      or   (p_lgvrfyflg is null and p_lgvrfydt is not null)	then
	    w_msg := 'Both logic verification flag and date must be provide or both must be blank.';
      else
	    w_msg := null;
	  end case;
	  --
	  open dtyp_cur(p_trgcldtyp);
	  fetch dtyp_cur into w_dtyp_rec;
	  close dtyp_cur;
	  --
	  if w_dtyp_rec.prval is null then
	     w_msg := 'The datatype '||p_trgcldtyp||' for '||p_trgclnm||' is invalid.'||chr(10)||
		          'Please verify parameters for "Datatype".';
	  end if;
	  --
	  --dbms_output.put_line('Basic validation complete.');
	  --
	  if w_msg is not null then
	     w_parm := w_parm||'::'||w_msg;
		 raise value_error;
	  end if;
	  --
	  --dbms_output.put_line('Debug step 1.');
	  --
	  if length(p_maplogic) <= 100 then
	     begin
	       open msql_cur(p_maplogic);
		   fetch msql_cur into w_msql_rec;
		   close msql_cur;
	     exception
	 	   when others then
	 	     PKGERR.RAISE_ERROR(g_name, w_procnm,'135', w_parm);
	     end;
	  end if;
	  --
	  begin
	    open  mapr_cur(p_mapref);
	    fetch mapr_cur into w_mapr_rec;
	    close mapr_cur;
	  exception
		when others then
		  PKGERR.RAISE_ERROR(g_name, w_procnm,'136', w_parm);
	  end;
	  --
	  --dbms_output.put_line('Debug step 2.');
	  --
	  if w_mapr_rec.mapref is null then
	     w_msg  := 'Inavlid mapping reference.';
		 w_parm := w_parm||'::'||w_msg;
		 raise value_error;
	  end if;
	  --
	  --dbms_output.put_line('Debug step 3.');
	  --
	  open maprdtl_cur(p_mapref, p_trgclnm);
	  fetch maprdtl_cur into w_maprdtl_rec;
	  close maprdtl_cur;
	  --
	  --dbms_output.put_line('Debug step 4.');
	  --
	  if w_maprdtl_rec.mapdtlid is not null then
		 --
		 if w_maprdtl_rec.mapref             != p_mapref   
         or w_maprdtl_rec.trgclnm            != p_trgclnm  
         or w_maprdtl_rec.trgcldtyp          != p_trgcldtyp
         or nvl(w_maprdtl_rec.trgkeyflg,'N') != nvl(p_trgkeyflg,'N')
         or nvl(w_maprdtl_rec.trgkeyseq,-1)  != nvl(p_trgkeyseq,-1)
         or w_maprdtl_rec.trgcldesc          != p_trgcldesc
         or w_maprdtl_rec.maplogic           != p_maplogic 
	     or w_maprdtl_rec.keyclnm            != p_keyclnm  
	     or w_maprdtl_rec.valclnm            != p_valclnm  
         or w_maprdtl_rec.mapcmbcd           != p_mapcmbcd 
         or w_maprdtl_rec.excseq             != p_excseq   
         or w_maprdtl_rec.scdtyp             != p_scdtyp   
         or w_maprdtl_rec.lgvrfyflg          != p_lgvrfyflg
         or w_maprdtl_rec.lgvrfydt           != p_lgvrfydt then
		    -- New record has changes, update required.
			w_chg := 'Y';
		 else
		    -- existing record is same as new record no change or insert required.
			-- return the existing id.
			w_chg := 'N';
			w_mapdtlid := w_maprdtl_rec.mapdtlid;
		 end if;
		 --
		 if w_chg = 'Y' then
			declare
			  w_pm varchar2(200) := w_parm||' Mapref='||to_char(w_maprdtl_rec.mapref)||
			                        ' Trgclnm='||w_maprdtl_rec.trgclnm;
			begin
			  update dwmaprdtl
			  set    curflg  = 'N'
			        ,recupdt = sysdate
					,uptdby  = g_user
			  where  mapref = w_maprdtl_rec.mapref
			  and    mapdtlid = w_maprdtl_rec.mapdtlid
			  and    curflg = 'Y';
			exception
			  when others then
				PKGERR.RAISE_ERROR(g_name, w_procnm,'105', w_pm);
			end;
		 end if;
	  end if;
	  --
	  -- Insert new record.
	  --
	  if w_chg = 'Y' then
		 begin
		   insert into 
		   dwmaprdtl (mapdtlid, mapref, trgclnm, trgcldtyp, trgkeyflg, trgkeyseq, trgcldesc
		             ,maplogic, maprsqlcd, keyclnm, valclnm, mapcmbcd, excseq, scdtyp, lgvrfyflg
				     ,lgvrfydt, reccrdt, recupdt, curflg, crtdby, uptdby)
		   values (dwmaprdtlseq.nextval,  p_mapref, p_trgclnm, p_trgcldtyp, p_trgkeyflg, p_trgkeyseq, p_trgcldesc
                  ,p_maplogic, w_msql_rec.dwmaprsqlcd, p_keyclnm, p_valclnm, p_mapcmbcd, p_excseq, p_scdtyp, p_lgvrfyflg
                  ,p_lgvrfydt, sysdate, sysdate, 'Y', g_user, g_user)
		   returning mapdtlid into w_mapdtlid;
		 exception
		   when others then
				PKGERR.RAISE_ERROR(g_name, w_procnm,'106', w_parm);
		 end;
	  end if;
	  --
	  return w_mapdtlid;
	  --
	exception 
	  when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'107', w_parm);
	end CREATE_UPDATE_MAPPING_DETAIL;
	
	--
	-- Function body to create or update mappings, returns mapping detail ID
	-- Any change is to be historised.
	--
	function CREATE_UPDATE_MAPPING_DETAIL(
       p_mapref     in dwmaprdtl.mapref%type
      ,p_trgclnm    in dwmaprdtl.trgclnm%type
      ,p_trgcldtyp  in dwmaprdtl.trgcldtyp%type
      ,p_trgkeyflg  in dwmaprdtl.trgkeyflg%type
      ,p_trgkeyseq  in dwmaprdtl.trgkeyseq%type
      ,p_trgcldesc  in dwmaprdtl.trgcldesc%type
      ,p_maplogic   in dwmaprdtl.maplogic%type
	  ,p_keyclnm    in dwmaprdtl.keyclnm%type
	  ,p_valclnm    in dwmaprdtl.valclnm%type
      ,p_mapcmbcd   in dwmaprdtl.mapcmbcd%type
      ,p_excseq     in dwmaprdtl.excseq%type
      ,p_scdtyp     in dwmaprdtl.scdtyp%type
      ,p_lgvrfyflg  in dwmaprdtl.lgvrfyflg%type
      ,p_lgvrfydt   in dwmaprdtl.lgvrfydt%type
      ,p_user       in dwmaprdtl.crtdby%type )  
    return dwmaprdtl.mapdtlid%type is
	  --
	  w_procnm  varchar2(50) := 'CREATE_UPDATE_MAPPING_DETAIL';
	  --
	  w_parm    varchar2(400) := substr('Mapref='||p_mapref||'-'||p_trgclnm||
	                                   ' User='||p_user,1,400);
	  w_mapdtlid dwmaprdtl.mapdtlid%type := null;
	  --
	begin
	  if p_user is null then
	     w_parm := w_parm||'::'||'Session user not provided.';
		 raise value_error;
	  end if;
	  --
	  g_user := p_user;
	  --
	  w_mapdtlid := PKGDWMAPR.CREATE_UPDATE_MAPPING_DETAIL(p_mapref   
                                                          ,p_trgclnm  
                                                          ,p_trgcldtyp
                                                          ,p_trgkeyflg
                                                          ,p_trgkeyseq
                                                          ,p_trgcldesc
                                                          ,p_maplogic 
	                                                      ,p_keyclnm  
	                                                      ,p_valclnm  
                                                          ,p_mapcmbcd 
                                                          ,p_excseq   
                                                          ,p_scdtyp   
                                                          ,p_lgvrfyflg
                                                          ,p_lgvrfydt );
      --
	  return w_mapdtlid;
	exception 
	  when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'108', w_parm);
	end CREATE_UPDATE_MAPPING_DETAIL;
	
	--
	-- private procedure to validate the SQL
	--
	procedure VALIDATE_SQL(p_logic   in  dwmaprsql.dwmaprsql%type
	                      ,p_keyclnm in  dwmaprdtl.keyclnm%type
						  ,p_valclnm in  dwmaprdtl.valclnm%type
						  ,p_flg     in  varchar2 default 'Y'
	                      ,p_error   out varchar2) is
      --
	  w_procnm  varchar2(50) := 'VALIDATE_SQL';
	  --
	  w_parm    varchar2(400) := substr('KayColum='||p_keyclnm||
	                                   ' ValColumn='||p_valclnm,1,400);	
      --
	  w_cursor  number(12)      := null;
	  w_rows    number(12)      := null;
	  w_sql     varchar2(32767) := null;
	  --
	  w_logic   clob;
	  --
	begin
	  if nvl(p_flg,'Y') = 'Y' then
	     case
	     when p_keyclnm is null then
	       p_error := 'Key column(s) not provided.';
	     when p_valclnm is null then
	       p_error := 'Key column(s) not provided.';
		 else
		   null;
		 end case;
	  end if;
	  --
      declare
		w_len number(20) := 0;
	  begin
	    w_len := nvl(DBMS_LOB.GETLENGTH(p_logic),0);
        --
        if w_len = 0 then
	       p_error := 'SQL provided is empty.';
	    end if;
	  exception
	    when others then
	      PKGERR.RAISE_ERROR(g_name, w_procnm,'137', w_parm);
	  end;
	  --
	  DBMS_LOB.CREATETEMPORARY(w_logic, True, 2);
	  --
	  if nvl(p_flg,'Y') = 'Y' then
	     w_sql := 'select '||p_keyclnm||','||p_valclnm||' from (';
	     --
	     DBMS_LOB.WRITEAPPEND(w_logic, length(w_sql), w_sql);
	     --
	     DBMS_LOB.APPEND(w_logic, p_logic);
	     --
	     w_sql := ') sql1 where rownum = 1';
	     --
	     DBMS_LOB.WRITEAPPEND(w_logic, length(w_sql), w_sql);
	  else
	     DBMS_LOB.APPEND(w_logic, p_logic);
	  end if;
	  --
	  w_logic := regexp_replace(upper(w_logic),'DWT_PARAM[0-9]','NULL');
	  w_logic := regexp_replace(upper(w_logic),';','');
	  --
	  dbms_output.put_line('w_logic = '||w_logic);
	  --
	  w_cursor := DBMS_SQL.OPEN_CURSOR;
	  --
	  begin
	    DBMS_SQL.PARSE(w_cursor, w_logic, dbms_sql.native);
	  exception
	    when others then
		  p_error := sqlerrm;
	  end;
	  --
	  /*
	  -- 28-Jun-2025, commented below section to improve validation performance
	  if p_error is null then
	     w_rows := DBMS_SQL.EXECUTE_AND_FETCH(w_cursor);
	     dbms_output.put_line('execute and fetch complete.');
	  end if;
	  */
	  --
	  if DBMS_SQL.IS_OPEN(w_cursor) then
	     DBMS_SQL.CLOSE_CURSOR(w_cursor);
	     --
	     dbms_output.put_line('close cursor complete.');
	  end if;
	  --
	  if nvl(DBMS_LOB.ISTEMPORARY(w_logic),0) > 0 then
	     DBMS_LOB.FREETEMPORARY(w_logic);
	  end if;
	  --
	exception
	  when others then
	    if DBMS_SQL.IS_OPEN(w_cursor) then
		   DBMS_SQL.CLOSE_CURSOR(w_cursor);
		end if;
		--
		if nvl(DBMS_LOB.ISTEMPORARY(w_logic),0) > 0 then
	       DBMS_LOB.FREETEMPORARY(w_logic);
		end if;
		--
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'138', w_parm);  
	end VALIDATE_SQL;
	
	--
    -- Function to validate SQL.
    --
    function VALIDATE_SQL(p_logic in dwmaprsql.dwmaprsql%type)
    return   varchar2 is
	  w_procnm  varchar2(50)  := 'VALIDATE_SQL';
	  w_parm    varchar2(200) := 'SQL Validate with Clob.';
	  w_err     varchar2(400) := null;
	  w_result varchar2(1)   := 'Y';
	begin
	  VALIDATE_SQL(p_logic, null, null, 'N', w_err);
	  --
	  if w_err is not null then
	     w_result := 'N';
	  else 
	     w_result := 'Y';
	  end if;
	  --
	  return w_result;
	  --
	exception
	  when others then  
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'139', w_parm);
	end VALIDATE_SQL;
  
  
	--
	-- Function to validate mapping logic.  Return Y/N.
	-- Y indicates, logic is valid
	-- N indicates, logic is invalid.
	--
	Function VALIDATE_LOGIC(p_logic   in dwmaprdtl.maplogic%type
						   ,p_keyclnm in dwmaprdtl.keyclnm%type
						   ,p_valclnm in dwmaprdtl.valclnm%type)
	return dwmaprdtl.lgvrfyflg%type is
	  --
	  w_procnm  varchar2(50) := 'VALIDATE_LOGIC';
	  --
	  w_parm    varchar2(400) := substr('KayColum='||p_keyclnm||
	                                   ' ValColumn='||p_valclnm||':'||p_logic,1,400);	
      --
	  cursor csr is
	  select dwmaprsqlcd, dwmaprsql
	  from   dwmaprsql
	  where  dwmaprsqlcd = substr(p_logic,1, 100)
	  and    curflg = 'Y';
	  --
	  w_rec  csr%rowtype;
	  --
	  w_logic   clob;
	  w_error   varchar2(32767) := null;
	  --
	  w_return  dwmaprdtl.lgvrfyflg%type := 'Y';
	begin
	  open csr;
	  fetch csr into w_rec;
	  close csr;
      --
	  DBMS_LOB.CREATETEMPORARY(w_logic, True, 2);
	  --
	  if w_rec.dwmaprsqlcd is null then
	     DBMS_LOB.WRITEAPPEND(w_logic, length(p_logic), p_logic);
	  else
         DBMS_LOB.APPEND(w_logic, w_rec.dwmaprsql);
      end if;		 
	  --
	  VALIDATE_SQL(w_logic, p_keyclnm, p_valclnm, 'Y', w_error);
	  --
	  if w_error is not null then
	     w_return := 'N';
	  end if;
	  --
	  DBMS_LOB.FREETEMPORARY(w_logic);
      --
	  return w_return;
	  --
	exception
      when others then
	    if nvl(DBMS_LOB.ISTEMPORARY(w_logic),0) > 0 then
	       DBMS_LOB.FREETEMPORARY(w_logic);
		end if;
		--
        PKGERR.RAISE_ERROR(g_name, w_procnm,'109', w_parm);
	end  VALIDATE_LOGIC;
	--
	-- Function to validate mapping logic.  Returns Y/N.
	-- Y indicates, logic is valid
	-- N indicates, logic is invalid.
	--
	Function VALIDATE_LOGIC2(p_logic   in  dwmaprdtl.maplogic%type
						    ,p_keyclnm in  dwmaprdtl.keyclnm%type
						    ,p_valclnm in  dwmaprdtl.valclnm%type
						    ,p_err     out varchar2)
	return dwmaprdtl.lgvrfyflg%type is
	  --
	  w_procnm  varchar2(50) := 'VALIDATE_LOGIC2';
	  --
	  w_parm    varchar2(400) := substr('KayColum='||p_keyclnm||
	                                   ' ValColumn='||p_valclnm||':'||p_logic,1,400);	
      --
      --
	  cursor csr is
	  select dwmaprsqlcd, dwmaprsql
	  from   dwmaprsql
	  where  dwmaprsqlcd = substr(p_logic,1, 100)
	  and    curflg = 'Y';
	  --
	  w_rec  csr%rowtype;
	  w_logic   clob;
	  --
	  w_return  dwmaprdtl.lgvrfyflg%type := 'Y';
	begin
	  open csr;
	  fetch csr into w_rec;
	  close csr;
      --
	  DBMS_LOB.CREATETEMPORARY(w_logic, True, 2);
	  --
	  if w_rec.dwmaprsqlcd is null then
	     DBMS_LOB.WRITEAPPEND(w_logic, length(p_logic), p_logic);
	  else
         DBMS_LOB.APPEND(w_logic, w_rec.dwmaprsql);
      end if;		 
	  --
	  VALIDATE_SQL(w_logic, p_keyclnm, p_valclnm, 'Y', p_err);
	  --
	  if p_err is not null then
	     w_return := 'N';
	  end if;
      --
	  DBMS_LOB.FREETEMPORARY(w_logic);
      --
	  return w_return;
	  --
	exception
      when others then
	    if nvl(DBMS_LOB.ISTEMPORARY(w_logic),0) > 0 then
	       DBMS_LOB.FREETEMPORARY(w_logic);
		end if;
		--
        PKGERR.RAISE_ERROR(g_name, w_procnm,'110', w_parm);
	end  VALIDATE_LOGIC2;
	
	--
	-- Function to validate all the mappings given for a mapping reference.
	-- Uses recent records from mappings table and updates logic validation columns.
	-- Internaly uses validate_logic function.
	-- Returns boolean flag
	-- false indicates atleast one logic has failed
	-- true indicates all the logics are valid.
    --
    Function VALIDATE_LOGIC(p_mapref in dwmapr.mapref%type)
	return  dwmaprdtl.lgvrfyflg%type is
	--
	  w_procnm  varchar2(50) := 'VALIDATE_LOGIC';
	  --
	  w_parm    varchar2(200) := substr('Mapref='||p_mapref,1,200);	
      --
	  cursor map_cur (c_mapref  dwmapr.mapref%type) is
	  select m.mapref,  md.mapdtlid, m.trgtbnm, md.trgclnm
	        ,md.keyclnm, md.valclnm, md.maplogic
	  from   dwmapr m
	        ,dwmaprdtl md
	  where  m.mapref = c_mapref
	  and    m.curflg = 'Y'
	  and    md.mapref = m.mapref
	  and    md.curflg = 'Y';
	  --
	  w_return  dwmaprdtl.lgvrfyflg%type := 'Y';
	begin
	  --
	  for map_rec in map_cur(p_mapref) loop 
	    --
	    declare 
		  w_pm  varchar2(400) := substr('TB:'||map_rec.trgtbnm||'-'||'TC:'||map_rec.trgclnm||':'||
		                                'Key:'||map_rec.keyclnm||'-'||'Val:'||map_rec.valclnm||'-'||
										map_rec.maplogic, 1, 400);
          w_res dwmaprdtl.lgvrfyflg%type := null;
		  w_err dwmaperr.errmsg%type := null;
	    begin
		  w_res := VALIDATE_LOGIC2(map_rec.maplogic, map_rec.keyclnm, map_rec.valclnm, w_err);
		  --
		  if w_res = 'N' and w_err is not null then
		     dbms_output.put_line('mapdtlid = '||map_rec.mapdtlid);
		     begin
			   insert into
			   dwmaperr(maperrid, mapdtlid, mapref, maplogic, errtyp, errmsg, reccrdt)
			   values (dwmaperrseq.nextval, map_rec.mapdtlid, map_rec.mapref, map_rec.maplogic, 'ERR', w_err, sysdate);
			 exception
			   when others then
			     PKGERR.RAISE_ERROR(g_name, w_procnm, '111', w_pm);
			 end;
		  end if;
		  
		  if w_return = 'Y' then
		     w_return := w_res;
		  end if;
		  --
		  update dwmaprdtl
		  set    lgvrfydt  = sysdate
		        ,lgvrfyflg = w_res
		  where mapref   = map_rec.mapref
		  and   mapdtlid = map_rec.mapdtlid
		  and   curflg   = 'Y';
		exception
		  when others then
		    PKGERR.RAISE_ERROR(g_name, w_procnm, '112', w_pm);
        end;		
	  end loop;
	  --
	  if w_return = 'Y' then
	     declare
   	       cursor c2_cur is
	       select valclnm, mapcmbcd, count(*) cnt
	       from   dwmaprdtl
	       where  curflg = 'Y'
	       and    mapref = p_mapref
	       having count(*) > 1
	       group by valclnm, mapcmbcd;
	       --
	       w_c2_rec c2_cur%rowtype       := null;
           w_err    dwmaperr.errmsg%type := null;
	     begin
	       open c2_cur;
	   	   fetch c2_cur into w_c2_rec;
	       close c2_cur;
	       --
	       if w_c2_rec.cnt > 1 then
	          w_err := 'Target value column name ('||w_c2_rec.valclnm||') cannot repeat within a mapping code(
	                   '||w_c2_rec.mapcmbcd||'). Please use alias if required.';
	          w_return := 'N';
	          --
	          begin
	       	   insert into
	     	   dwmaperr(maperrid, mapdtlid, mapref, maplogic, errtyp, errmsg, reccrdt)
	     	   values (dwmaperrseq.nextval, null, p_mapref, null, 'ERR', w_err, sysdate);
	     	 exception
	     	   when others then
	     	     PKGERR.RAISE_ERROR(g_name, w_procnm, '127', w_parm);
	     	 end;
	       end if;
	     exception
	       when others then
   	         PKGERR.RAISE_ERROR(g_name, w_procnm,'128', w_parm);
	     end;
	  end if;
	  --
	  if w_return = 'Y' then
	     declare
   	       cursor c2_cur is
	       select maprsqlcd, mapcmbcd, count(*) cnt
		   from (select distinct maprsqlcd, mapcmbcd
	             from   dwmaprdtl
	             where  curflg = 'Y'
	             and    mapref = p_mapref) x
	       group by maprsqlcd, mapcmbcd
	       having count(*) > 1;
	       --
	       w_c2_rec c2_cur%rowtype       := null;
           w_err    dwmaperr.errmsg%type := null;
	     begin
	       open c2_cur;
	   	   fetch c2_cur into w_c2_rec;
	       close c2_cur;
	       --
	       if w_c2_rec.cnt > 1 then
	          w_err := 'For a "Mapping Combination"/"SQL Query Code", more than 1 "SQL Query Code"/"Mapping Combination" is not allowed';
	          w_return := 'N';
	          --
	          begin
	       	   insert into
	     	   dwmaperr(maperrid, mapdtlid, mapref, maplogic, errtyp, errmsg, reccrdt)
	     	   values (dwmaperrseq.nextval, null, p_mapref, null, 'ERR', w_err, sysdate);
	     	 exception
	     	   when others then
	     	     PKGERR.RAISE_ERROR(g_name, w_procnm, '140', w_parm);
	     	 end;
	       end if;
	     exception
	       when others then
   	         PKGERR.RAISE_ERROR(g_name, w_procnm,'141', w_parm);
	     end;
	  end if;
	  --
	  -- If all logic is verified then
	  -- Update mapping record as verification complete.
	  --
	  if w_return = 'Y' then
	     declare 
		  w_pm  varchar2(400) := substr('MapRef='||p_mapref, 1, 400);
	     begin
		   update dwmapr
		   set   lgvrfydt  = sysdate
		        ,lgvrfyflg = w_return
				,lgvrfby   = g_user
		   where mapref  = p_mapref
		   and   curflg = 'Y';
		 exception
		   when others then
		     PKGERR.RAISE_ERROR(g_name, w_procnm, '113', w_pm);
         end;
	  end if;
	  --
	  return w_return;
	  --
	exception
      when others then
        PKGERR.RAISE_ERROR(g_name, w_procnm,'129', w_parm);
	end VALIDATE_LOGIC;
	
	--
	-- Function to validate all the mappings given for a mapping reference.
	-- Uses recent records from mappings table and updates logic validation columns.
	-- Internaly uses validate_logic function.
	-- Returns boolean flag
	-- false indicates atleast one logic has failed
	-- true indicates all the logics are valid.
    --
    Function VALIDATE_LOGIC(p_mapref in dwmapr.mapref%type
	                       ,p_user   in dwmapr.crtdby%type)
	return  dwmaprdtl.lgvrfyflg%type is
	--
	  w_procnm  varchar2(50) := 'VALIDATE_LOGIC';
	  --
	  w_parm    varchar2(200) := substr('Mapref='||p_mapref||
	                                   ' User='||p_user,1,200);	
	  --
	  w_return  dwmaprdtl.lgvrfyflg%type := 'Y';
    begin
	  if p_user is null then
	     w_parm := w_parm||'::'||'Session user not provided.';
		 raise value_error;
	  end if;
	  --
	  g_user := p_user;
	  --
	  w_return := PKGDWMAPR.VALIDATE_LOGIC(p_mapref);
	  --
	  return w_return;
	  --
	exception
      when others then
        PKGERR.RAISE_ERROR(g_name, w_procnm,'114', w_parm);
	end VALIDATE_LOGIC;  
	
	--
	-- Function to validate mapping details.
	--
	function VALIDATE_MAPPING_DETAILS (p_mapref in  dwmapr.mapref%type
	                                  ,p_err    out varchar2)
	return   varchar2 is
	  --
	  w_procnm  varchar2(50) := 'VALIDATE_MAPPING_DETAILS';
	  --
	  w_parm    varchar2(200) := substr('Mapref='||p_mapref,1,200);
	  --
	  w_msg     varchar2(400) := null;
	  --
	  cursor pk_cur is
	  select trgkeyseq, count(*) cnt
      from dwmaprdtl
      where curflg = 'Y'
      and   mapref = p_mapref
	  and   trgkeyflg = 'Y'
      group by trgkeyseq;
	  --
	  w_pk_rec pk_cur%rowtype;
	  --
	  cursor cl_cur is
	  select trgclnm, count(*) cnt
      from dwmaprdtl
      where curflg = 'Y'
      and   mapref = p_mapref
      group by trgclnm;
	  --
	  w_cl_rec cl_cur%rowtype := null;
	  --
	  cursor c2_cur is
	  select valclnm, mapcmbcd, count(*) cnt
	  from   dwmaprdtl
	  where  curflg = 'Y'
	  and    mapref = p_mapref
	  having count(*) > 1
	  group by valclnm, mapcmbcd;
	  --
	  w_c2_rec c2_cur%rowtype := null;
	  --
	  w_return varchar2(1) := 'Y';
	  --
	begin
	  --
	  declare
	    w_flg varchar2(1) := 'N';
	  begin
	    w_flg := VALIDATE_LOGIC(p_mapref);
		--
		if nvl(w_flg,'N') = 'N' then 
		   w_msg := 'Some/All target columns logic validation failed, please verify logic(SQL).';
		   w_return := 'N';
		end if;
	  exception
	    when others then
   	      PKGERR.RAISE_ERROR(g_name, w_procnm,'115', w_parm);
	  end;
	  --
	  if w_msg is null then
	     begin
	       open pk_cur;
	       fetch pk_cur into w_pk_rec;
	       close pk_cur;
	       --
		   if w_msg is null and nvl(w_pk_rec.cnt,0) = 0 then
		      w_msg := 'Primary key not specified, primary key(s) is manadatory.';
		  	  w_return := 'N';
		   end if;
		   --
	       if w_msg is null and w_pk_rec.trgkeyseq is not null and w_pk_rec.cnt > 1 then
	          w_msg := 'Primary sequence cannot repeat within mapping.';
		  	  w_return := 'N';
	       end if;
		 exception
		   when others then
   	         PKGERR.RAISE_ERROR(g_name, w_procnm,'125', w_parm);
	     end;
	  end if;
	  --
	  if w_msg is null then
	     begin
	       open cl_cur;
		   fetch cl_cur into w_cl_rec;
		   close cl_cur;
		   --
		   if w_cl_rec.cnt > 1 then
		      w_msg := 'Target column name cannot repeat within mapping.';
		  	  w_return := 'N';
	       end if;
		 exception
		   when others then
   	         PKGERR.RAISE_ERROR(g_name, w_procnm,'126', w_parm);
	     end;
	  end if;
	  --
	  if w_msg is null then
	     begin
	       open c2_cur;
		   fetch c2_cur into w_c2_rec;
		   close c2_cur;
		   --
		   if w_c2_rec.cnt > 1 then
		      w_msg := 'Target value column name ('||w_c2_rec.valclnm||') cannot repeat within a mapping code(
			           '||w_c2_rec.mapcmbcd||'). Please use alias if required.';
		  	  w_return := 'N';
	       end if;
		 exception
		   when others then
   	         PKGERR.RAISE_ERROR(g_name, w_procnm,'130', w_parm);
	     end;
	  end if;
	  --
	  if w_msg is not null then
	     w_parm := w_parm||'::'||w_msg;
		 --raise value_error;
	  end if;
	  --
	  p_err := w_msg;
	  --
	  return w_return;
	  --
	exception
	  when others then
   	    PKGERR.RAISE_ERROR(g_name, w_procnm,'116', w_parm);
	end VALIDATE_MAPPING_DETAILS;
	
	--
	-- Function to validate mapping details.
	--
	function VALIDATE_MAPPING_DETAILS (p_mapref in  dwmapr.mapref%type
	                                  ,p_user   in  dwmapr.crtdby%type
	                                  ,p_err    out varchar2)
	return   varchar2 is
	  --
	  w_procnm  varchar2(50) := 'VALIDATE_MAPPING_DETAILS';
	  --
	  w_parm    varchar2(200) := substr('Mapref='||p_mapref,1,200);
	  --
	  w_return varchar2(1) := 'Y';
	  --
	begin
	  if p_user is null then
	     w_parm := w_parm||'::'||'Session user not provided.';
		 raise value_error;
	  end if;
	  --
	  g_user := p_user;
	  --
	  w_return := PKGDWMAPR.VALIDATE_MAPPING_DETAILS(p_mapref, p_err);
	  --
	  return w_return;
	  --
	exception
	  when others then
   	    PKGERR.RAISE_ERROR(g_name, w_procnm,'117', w_parm);
	end VALIDATE_MAPPING_DETAILS;  
	
	--
	-- Procedure to activate or deactivate a mapping.
	--
	Procedure ACTIVATE_DEACTIVATE_MAPPING(p_mapref in  dwmapr.mapref%type
                                         ,p_stflg  in  dwmapr.stflg%type
										 ,p_err    out varchar2)
    is
	  --
	  w_procnm  varchar2(50) := 'ACTIVATE_DEACTIVATE_MAPPING';
	  --
	  w_parm    varchar2(200) := substr('Mapref='||p_mapref,1,200);
	  --
	  w_msg      varchar2(200) := null;
	  -- 
	begin
	  --
	  if nvl(p_stflg ,'N') not in ('A','N') then
	     w_msg := 'Invalid status flag (valid: A or N).';
	  end if;
	  --
	  -- Check whether mappings are correct.
	  --
	  if p_stflg = 'A' and w_msg is null then
	     declare
	       w_flg varchar2(1) := 'N';
	     begin
	       w_flg := VALIDATE_MAPPING_DETAILS(p_mapref, w_msg);
	 	   --
	 	   if nvl(w_flg,'N') = 'N' then 
	 	      w_msg := w_msg||chr(10)||'Cannot activate mapping few columns logic failed.';
	 	   end if;
	     exception 
	       when others then
	         PKGERR.RAISE_ERROR(g_name, w_procnm,'118', w_parm);
	     end;
		 --
		 -- If any error found, no chage to be applied to mapping.
		 --
		 if w_msg is null then
	        update dwmapr
	        set    stflg = p_stflg
			      ,actby = g_user
				  ,actdt = sysdate
	        where  mapref = p_mapref
	        and    curflg = 'Y';
	     end if;
	     --
	  end if;
	  --
	  if w_msg is not null then
	     p_err := w_msg;
	     w_parm := w_parm||'::'||w_msg;
		  --raise value_error;
	  end if;
	  --
	exception 
	  when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'119', w_parm);
	end ACTIVATE_DEACTIVATE_MAPPING;

--
	-- Procedure to activate or deactivate a mapping.
	--
	Procedure ACTIVATE_DEACTIVATE_MAPPING(p_mapref in  dwmapr.mapref%type
                                         ,p_stflg  in  dwmapr.stflg%type
										 ,p_user   in  dwmapr.crtdby%type
										 ,p_err    out varchar2)
    is
	  --
	  w_procnm  varchar2(50) := 'ACTIVATE_DEACTIVATE_MAPPING';
	  --
	  w_parm    varchar2(200) := substr('Mapref='||p_mapref,1,200);
	  --
	begin
	  if p_user is null then
	     w_parm := w_parm||'::'||'Session user not provided.';
		 raise value_error;
	  end if;
	  --
	  g_user := p_user;
	  --
	  PKGDWMAPR.ACTIVATE_DEACTIVATE_MAPPING(p_mapref, p_stflg, p_err);
	  --
	exception 
	  when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'120', w_parm);
	end ACTIVATE_DEACTIVATE_MAPPING;
	
    --
    -- Procedure to delete mapping.
    --
    procedure DELETE_MAPPING(p_mapref in  dwmapr.mapref%type
	                        ,p_err    out varchar2)
    is
	  --
	  w_procnm  varchar2(50) := 'DELETE_MAPPING';
	  --
	  w_parm    varchar2(200) := substr('Mapref='||p_mapref,1,200);
	  --
	  cursor job_csr is
	  select mapref, jobid
	  from   dwjob
	  where  mapref = p_mapref
	  and    curflg = 'Y';
	  --
	  w_job_rec job_csr%rowtype := null;
	  --
	begin
	  open  job_csr;
	  fetch job_csr into w_job_rec;
	  close job_csr;
	  --
	  if w_job_rec.jobid is not null then
	     p_err := 'The mapping "'||p_mapref||'" cannot be deleted becuase related job exists.';
	  else
	     begin
		   delete from dwmaprdtl
		   where  mapref = p_mapref;
		   --
		   delete from dwmapr
		   where  mapref = p_mapref;
		   --
		   commit;
		   --
		 exception
		   when others then
	         PKGERR.RAISE_ERROR(g_name, w_procnm,'121', w_parm);
		 end;
	  end if;
	     
	exception 
	  when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'122', w_parm);
	end DELETE_MAPPING;
	
    --
    -- Procedure to delete mapping details.
    --
    procedure DELETE_MAPPING_DETAILS(p_mapref  in  dwmaprdtl.mapref%type 
                                    ,p_trgclnm in  dwmaprdtl.trgclnm%type
								    ,p_err     out varchar2)
	is
	  --
	  w_procnm  varchar2(50) := 'DELETE_MAPPING_DETAILS';
	  --
	  w_parm    varchar2(200) := substr('Mapref='||p_mapref||
	                                   ' Trgclnm='||p_trgclnm,1,200);
      --
      cursor jd_csr is
	  select mapref, jobdtlid
	  from   dwjobdtl
	  where  mapref  = p_mapref
	  and    trgclnm = p_trgclnm
	  and    curflg  = 'Y';
	  --
	  w_jd_rec jd_csr%rowtype := null; 
	begin
	  open  jd_csr;
	  fetch jd_csr into w_jd_rec;
	  close jd_csr;
	  --
	  if w_jd_rec.jobdtlid is not null then
	     p_err := 'The mapping detail for "'||p_mapref||'-'||p_trgclnm||'" cannot be deleted becuase related job detail exists.'; 
	  else
	     begin
		   delete from dwmaprdtl
		   where  mapref  = p_mapref
		   and    trgclnm = p_trgclnm;
		   --
		   commit;
		   --
		 exception
		   when others then
	         PKGERR.RAISE_ERROR(g_name, w_procnm,'123', w_parm);
		 end;
	  end if;
	exception 
	  when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'124', w_parm);
	end DELETE_MAPPING_DETAILS;
	
	
end PKGDWMAPR;
/
