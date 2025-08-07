--
-- Package body for processing Dim date records.
--

Create or replace package body PKGDIMDATE is

	g_name constant varchar2(10) := 'PKGDIMDATE';
	g_ver  constant varchar2(10) := 'V001';
	--
	function VERSION return varchar is
	begin
	  return g_name||';'||g_ver;
	end VERSION;
	
    --
    -- Procedure to load Date dimension records.
    --
    procedure LOAD_DIM_DATE(p_startdt  in Date
                           ,p_enddt    in date) is
	  --
	  w_procnm  varchar2(50)  := 'LOAD_DIM_DATE';
	  --
	  w_parm    varchar2(400) := substr('StartDt='||to_char(p_startdt,'DD-Mon-YYYY')||
	                                   ' EndDt='||to_char(p_enddt,'DD-Mon-YYYY'), 1, 400);
	  --
	  type dim_date_tabtyp is table of dim_date%rowtype index by binary_integer;
	  w_dim_date_tb dim_date_tabtyp;
	  --
	  w_msg     varchar2(200) := null;
	  w_date    date          := p_startdt;
	  i         integer       := 0;
    begin
	  case
      when p_startdt is null then
	     w_msg := 'Start Date cannot be blank.';
	  when p_enddt is null then
	     w_msg := 'End date cannot be blank.';
	  when p_enddt <= p_startdt then
	     w_msg := 'End date must be after start date.';
	  else 
	     null;
	  end case;
	  --
	  if w_msg is not null then
	     w_parm := substr(w_parm||':'||w_msg,1,400);
	     Raise value_error;
	  end if;
	  --
	  loop
	    if w_date > p_enddt then
		   exit;
		end if;
		--
		i := i + 1;
		--
		declare
	      w_pm varchar2(100) := to_char(w_date,'DD-Mon-YYYY');
	    begin
		  w_dim_date_tb(i).the_date               := w_date;
          w_dim_date_tb(i).day_of_week            := to_char(w_date,'Dy');
          w_dim_date_tb(i).day_of_week_full       := to_char(w_date,'Day');
          w_dim_date_tb(i).dayno_of_the_week      := mod(to_char(w_date,'d')+5,7)+1;
          w_dim_date_tb(i).weekno_of_the_month    := to_number(to_char(w_date,'w'));
          w_dim_date_tb(i).weekno_of_the_year     := to_number(to_char(w_date,'iw'));
          w_dim_date_tb(i).week_start_date        := trunc(w_date,'IW');
          w_dim_date_tb(i).week_end_date          := trunc(w_date,'IW')+6;
          w_dim_date_tb(i).the_month              := to_char(w_date,'MM');
          w_dim_date_tb(i).dayno_of_the_month     := to_char(w_date,'DD');
          w_dim_date_tb(i).month_name             := to_char(w_date,'Mon');
		  w_dim_date_tb(i).month_name_full        := to_char(w_date,'Month');
          w_dim_date_tb(i).month_start_date       := trunc(w_date,'MM');
          w_dim_date_tb(i).month_end_date         := LAST_DAY(w_date);
          w_dim_date_tb(i).the_quater             := to_number(to_char(w_date,'Q'));
		  w_dim_date_tb(i).dayno_of_the_quarter   := w_date - trunc(w_date,'Q') +1;
          w_dim_date_tb(i).quarter_start_date     := trunc(w_date,'Q');
          w_dim_date_tb(i).quarter_end_date       := add_months(trunc(w_date,'Q'),3) -1;
          w_dim_date_tb(i).the_year               := to_char(w_date,'YYYY');
          w_dim_date_tb(i).dayno_of_the_year      := to_number(to_char(w_date,'DDD'));
          w_dim_date_tb(i).year_start_date        := trunc(w_date,'YYYY');
          w_dim_date_tb(i).year_end_date          := add_months(trunc(w_date,'YYYY'),12)-1;
          w_dim_date_tb(i).fiscal_day             := add_months(w_date, -3) - trunc(add_months(w_date,-3),'YEAR') +1;
          w_dim_date_tb(i).fiscal_period          := to_char(add_months(w_date, -3),'MM');
          w_dim_date_tb(i).fiscal_quarter         := to_number(to_char(add_months(w_date, -3),'Q'));
          w_dim_date_tb(i).fiscal_year_start      := to_char(trunc(add_months(w_date,-3),'YEAR'),'YYYY');
          w_dim_date_tb(i).fiscal_year_end        := to_char(add_months(trunc(add_months(w_date,-3),'YEAR'),12),'YYYY');
          w_dim_date_tb(i).fiscal_year_full       := w_dim_date_tb(i).fiscal_year_start||'-'||w_dim_date_tb(i).fiscal_year_end;
          w_dim_date_tb(i).fiscal_year_start_date := to_date('01-04-'||to_char(add_months(w_date,-3),'YYYY'),'DD-MM-YYYY');
          w_dim_date_tb(i).fiscal_year_end_date   := add_months(w_dim_date_tb(i).fiscal_year_start_date,12) -1;
          w_dim_date_tb(i).leap_year_flag         := case 
        		                                     when mod(to_number(to_char(w_date,'YYYY')),400) = 0 then
													   'Y'
													 when mod(to_number(to_char(w_date,'YYYY')),100) = 0 then
													   'N'
													 when mod(to_number(to_char(w_date,'YYYY')),4)   = 0 then
													   'Y'
													 else
													   'N'
													 end;
          w_dim_date_tb(i).weekend_indicator      := case when w_dim_date_tb(i).dayno_of_the_week in (1,7) then 'Y' else 'N' end;
          w_dim_date_tb(i).holiday_flag           := case 
		                                             when w_dim_date_tb(i).dayno_of_the_week = 7 then
													   'Y'
		                                             when w_dim_date_tb(i).weekno_of_the_month in (2,4)
													 and  w_dim_date_tb(i).dayno_of_the_week = 6 then 
													   'Y' 
													 else 
													   'N'
												     end;		  
	    exception
	      when others then
	        PKGERR.RAISE_ERROR(g_name, w_procnm,'101', w_parm);
		end;
		--
		w_date := w_date + 1;
		--
	  end loop;
	  --
	  -- Now load the data to table.
	  --
	  begin
	    forall i in w_dim_date_tb.first .. w_dim_date_tb.last 
		  insert into 
		  dim_date(skey
		          ,the_date
                  ,day_of_week
                  ,day_of_week_full
                  ,dayno_of_the_week
                  ,weekno_of_the_month
                  ,weekno_of_the_year
                  ,week_start_date
                  ,week_end_date
                  ,the_month
                  ,dayno_of_the_month
                  ,month_name
                  ,month_name_full
                  ,month_start_date
                  ,month_end_date
                  ,the_quater
                  ,dayno_of_the_quarter
                  ,quarter_start_date
                  ,quarter_end_date
                  ,the_year
                  ,dayno_of_the_year
                  ,year_start_date
                  ,year_end_date
                  ,fiscal_day
                  ,fiscal_period
                  ,fiscal_quarter
                  ,fiscal_year_start
                  ,fiscal_year_end
                  ,fiscal_year_full
                  ,fiscal_year_start_date
                  ,fiscal_year_end_date
                  ,leap_year_flag
				  ,weekend_indicator
				  ,holiday_flag
				  ,rec_cr_dt
				  ,rec_up_dt)
		  values (DIM_DATE_SEQ.nextval
		         ,w_dim_date_tb(i).the_date
		         ,w_dim_date_tb(i).day_of_week
		         ,w_dim_date_tb(i).day_of_week_full
		         ,w_dim_date_tb(i).dayno_of_the_week
		         ,w_dim_date_tb(i).weekno_of_the_month
		         ,w_dim_date_tb(i).weekno_of_the_year
		         ,w_dim_date_tb(i).week_start_date
		         ,w_dim_date_tb(i).week_end_date
		         ,w_dim_date_tb(i).the_month
		         ,w_dim_date_tb(i).dayno_of_the_month
		         ,w_dim_date_tb(i).month_name
		         ,w_dim_date_tb(i).month_name_full
		         ,w_dim_date_tb(i).month_start_date
		         ,w_dim_date_tb(i).month_end_date
		         ,w_dim_date_tb(i).the_quater
		         ,w_dim_date_tb(i).dayno_of_the_quarter
		         ,w_dim_date_tb(i).quarter_start_date
		         ,w_dim_date_tb(i).quarter_end_date
		         ,w_dim_date_tb(i).the_year
		         ,w_dim_date_tb(i).dayno_of_the_year
		         ,w_dim_date_tb(i).year_start_date
		         ,w_dim_date_tb(i).year_end_date
		         ,w_dim_date_tb(i).fiscal_day
		         ,w_dim_date_tb(i).fiscal_period
		         ,w_dim_date_tb(i).fiscal_quarter
		         ,w_dim_date_tb(i).fiscal_year_start
		         ,w_dim_date_tb(i).fiscal_year_end
		         ,w_dim_date_tb(i).fiscal_year_full
		         ,w_dim_date_tb(i).fiscal_year_start_date
		         ,w_dim_date_tb(i).fiscal_year_end_date
		         ,w_dim_date_tb(i).leap_year_flag
		  		 ,w_dim_date_tb(i).weekend_indicator
		         ,w_dim_date_tb(i).holiday_flag
				 ,sysdate
				 ,sysdate); 
      exception
	    when others then
	      PKGERR.RAISE_ERROR(g_name, w_procnm,'102', w_parm);
	  end				 
      --
      commit;				 
	  --
    exception
	  when others then
	    PKGERR.RAISE_ERROR(g_name, w_procnm,'103', w_parm);
	end LOAD_DIM_DATE;

	
end PKGDIMDATE;
/