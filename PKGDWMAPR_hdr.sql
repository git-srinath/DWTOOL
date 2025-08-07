--
-- Package for validating and processing mappings provided.
--
create or replace package PKGDWMAPR is
  --
  -- Function to get this package version.
  --
  function version return varchar;

  --
  -- Function to create mappings, returns mapping ID
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
  return dwmapr.mapid%type;
  --
  -- Function to create mappings, returns mapping ID
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
  return dwmapr.mapid%type;

  --
  -- Function to create mapping details, returns mapping details ID
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
  return dwmaprdtl.mapdtlid%type;
  
  --
  -- Function to create mapping details, returns mapping details ID
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
    ,p_user       in dwmaprdtl.crtdby%type	)  
  return dwmaprdtl.mapdtlid%type;
  
  --
  -- Function to validate the given sql logic.
  --
  Function VALIDATE_LOGIC(p_logic   in dwmaprdtl.maplogic%type
						 ,p_keyclnm in dwmaprdtl.keyclnm%type
						 ,p_valclnm in dwmaprdtl.valclnm%type)
  return dwmaprdtl.lgvrfyflg%type;
  
  --
  -- Function to validate all mapping logic for a mapping reference.
  --
  Function VALIDATE_LOGIC(p_mapref in dwmapr.mapref%type)
  return  dwmaprdtl.lgvrfyflg%type;
  
  --
  -- Function to validate all mapping logic for a mapping reference.
  --
  Function VALIDATE_LOGIC(p_mapref in dwmapr.mapref%type
                         ,p_user   in dwmapr.crtdby%type)
  return  dwmaprdtl.lgvrfyflg%type;
  
  --
  -- Function to validate mapping logic.  Returns Y/N.
  -- Y indicates, logic is valid
  -- N indicates, logic is invalid.
  --
  Function VALIDATE_LOGIC2(p_logic   in  dwmaprdtl.maplogic%type
  					      ,p_keyclnm in  dwmaprdtl.keyclnm%type
  					      ,p_valclnm in  dwmaprdtl.valclnm%type
  					      ,p_err     out varchar2)
  return dwmaprdtl.lgvrfyflg%type;
  
  --
  -- Funtion to validate details of the mapping.
  -- Must be called after CREATE_UPDATE_MAPPING_DETAIL is completed for all mappings.
  --
  Function VALIDATE_MAPPING_DETAILS (p_mapref in dwmapr.mapref%type
	                                ,p_err    out varchar2)
  return   varchar2;
  
  --
  -- Funtion to validate details of the mapping.
  -- Must be called after CREATE_UPDATE_MAPPING_DETAIL is completed for all mappings.
  --
  Function VALIDATE_MAPPING_DETAILS (p_mapref in dwmapr.mapref%type
                                    ,p_user   in dwmapr.crtdby%type
	                                ,p_err    out varchar2)
  return   varchar2;
  
  --
  -- Procedure to activate or deactivate a mapping.
  --
  Procedure ACTIVATE_DEACTIVATE_MAPPING(p_mapref in dwmapr.mapref%type
                                       ,p_stflg  in dwmapr.stflg%type
									   ,p_err    out varchar2);

  --
  -- Procedure to activate or deactivate a mapping.
  --
  Procedure ACTIVATE_DEACTIVATE_MAPPING(p_mapref in dwmapr.mapref%type
                                       ,p_stflg  in dwmapr.stflg%type
									   ,p_user   in dwmapr.crtdby%type
									   ,p_err    out varchar2);
									   
  --
  -- Procedure to delete mapping.
  --
  procedure DELETE_MAPPING(p_mapref in  dwmapr.mapref%type
                          ,p_err    out varchar2);
  
  --
  -- Procedure to delete mapping details.
  --
  procedure DELETE_MAPPING_DETAILS(p_mapref  in  dwmaprdtl.mapref%type 
                                  ,p_trgclnm in  dwmaprdtl.trgclnm%type
								  ,p_err     out varchar2);

end PKGDWMAPR;
/
