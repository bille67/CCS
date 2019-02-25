create or replace package body                             pkg_pathway_steps_2 as
TYPE ref_cur IS REF CURSOR;
/****************************************************************************************/
procedure ins_pkg_pathway_step_error( p_table_name in varchar2,
p_pathway_step in pkg_pathway_step_errors.pathway_step%TYPE,
p_error_message in pkg_pathway_step_errors.error_message%TYPE, 
p_pathway_table_id in pkg_pathway_step_errors.pathway_table_id%type default null,
p_visitid in pkg_pathway_step_errors.visit_id%TYPE, 
p_ispct pkg_pathway_step_errors.IS_PCT%TYPE)  AS
PRAGMA AUTONOMOUS_TRANSACTION;
begin

  insert into pkg_pathway_step_errors(pathway_table_name, pathway_step, error_message, error_date, pathway_table_id,
  is_pct, visit_id )
  values( p_table_name, p_pathway_step, p_error_message, SYSDATE, p_pathway_table_id, p_ispct, p_visitid);
  
  COMMIT;
end ins_pkg_pathway_step_error;
/****************************************************************************************/
procedure ins_pkg_pathway_code_error(p_status_id in PKG_PATHWAY_CODE_ERRORS.CLIENT_STEP_STATUS_ID%TYPE,
p_client_id in PKG_PATHWAY_CODE_ERRORS.client_id%TYPE, p_pathway_id in PKG_PATHWAY_CODE_ERRORS.pathway_id%TYPE,
p_pathway_step in PKG_PATHWAY_CODE_ERRORS.PATHWAY_STEP%TYPE, p_visit_id in PKG_PATHWAY_CODE_ERRORS.VISIT_ID%TYPE,
p_step_date in PKG_PATHWAY_CODE_ERRORS.step_date%TYPE,
p_error_message in pkg_pathway_code_errors.error_message%TYPE,
p_is_pct in pkg_pathway_code_errors.is_pct%TYPE)  AS
PRAGMA AUTONOMOUS_TRANSACTION;
begin

 insert into PKG_PATHWAY_CODE_ERRORS( CLIENT_STEP_STATUS_ID, CLIENT_ID, 
                      PATHWAY_ID, PATHWAY_STEP,VISIT_ID, STEP_DATE,
                      ERROR_MESSAGE, ERROR_DATE, IS_PCT)
        VALUES( p_status_id, p_client_id, p_pathway_id, p_pathway_step, p_visit_id, p_step_date,
        p_error_message, SYSDATE, p_is_pct);
  
  COMMIT;
end ins_pkg_pathway_code_error;

/***************************************************************
ins_client_step_status
inserts client_step_status_2 records
If the step has the appt_dates_logic_flag set in HUB_SM_2 it
will insert step status records for each appointment date
If the appt_dates_logic_flag is not set it will insert a step with the passed in
step date (normally the record updated date) and the complated date (which uses HUB_STEP_CLOSE_2)
***************************************************************/
procedure ins_client_step_status( p_client_id clients_pathways.CLIENTID%TYPE,
p_pathway_id  IN HUB_SM_2.PATHWAY_ID%TYPE,
p_pathway_step IN HUB_SM_2.pathway_step%TYPE,
p_visitid IN VARCHAR2,
p_step_date  IN DATE,
p_plan_id IN HUB_SM_2.plan_id%type,
p_is_pct IN client_step_status_2.is_pct%TYPE,
p_assignto IN client_step_status_2.ASSIGNTO%type,
p_completed_date DATE,
p_isbillable HUB_SM_2.isbillable%TYPE,
p_hubid IN client_step_status_2.hubid%TYPE,
p_agencyid in client_step_status_2.agencyid%TYPE,
p_appt_dates_logic_flag in hub_sm_2.appt_dates_logic_flag%TYPE,
p_id client_step_status_2.PK_ID%TYPE,
p_zip client_step_status_2.ZIP_CODE%TYPE,
p_census_tract client_step_status_2.census_tract%TYPE,
p_enroll_date client_step_status_2.ENROLLMENT_DATE%TYPE,
p_enroll_status client_step_status_2.ENROLL_STATUS%TYPE,
p_member_type client_step_status_2.MEMBER_TYPE%TYPE,
p_appt_type_token HUB_SM_2.APPT_TYPE_TOKEN%TYPE,
p_research_cat_tier1 client_step_status_2.research_cat_tier1%TYPE,
p_research_cat_tier2 client_step_status_2.research_cat_tier2%TYPE
) AS
l_count int := 0;

BEGIN
  dbms_output.put_line('ins_client_step_status: Client_id='||p_client_id||' pathway_id='||p_pathway_id||' p_pathway_step='||p_pathway_step);
   -- We potentially need to insert multiple client_step_status records for steps 123 & 90123
   -- For these we ned to refer to client_pathway_appt_dates using the visit_id and insert a row
   -- for each date we find there (that isn't already n client_step_status)
  -- if p_pathway_step in (123,/*223,*/90123) then 
  if p_appt_dates_logic_flag = 'Y' then
    for l_rec in ( select * from client_pathway_appt_dates where client_pathway_visitid = p_visitid and appt_type like '%'||p_appt_type_token ||'%'
                   and appt_date < SYSDATE order by appt_date)
    loop
      select count(*) into l_count
      from CLIENT_STEP_STATUS_2
      where pathway_id = p_pathway_id
      and client_id = p_client_id
      and visit_id = p_visitid
      and pathway_step = p_pathway_step
      --and step_date = l_rec.appt_date
      and completed_date = l_rec.appt_date;
      
      if l_count = 0 then
           insert into 
          CLIENT_STEP_STATUS_2( CLIENT_ID, PATHWAY_ID, PATHWAY_STEP, VISIT_ID, STEP_DATE, PLAN_ID, IS_PCT, ASSIGNTO, completed_date, 
          isbillable, hubid, agencyid, pk_id, ZIP_CODE, census_tract, ENROLLMENT_DATE, ENROLL_STATUS, MEMBER_TYPE, RESEARCH_CAT_TIER1, 
          RESEARCH_CAT_TIER2,
          HHTA_START_DATE, HHTA_SERVICE_DATE, HHTA_HH_G_CODE, HHTA_SERVICE_LOC, HHTA_APPROVAL_CODE, HHTA_CONSENT, HHTA_CC_NOTE,
          HHTA_ID_TIER, HHTA_ID_LOC, HHTA_HAP_CODE, HHTA_EXCEPTION, HHTA_INITIALS , HHTA_ASSESSMENTS , HHTA_QA_ADV_DIR, 
          HHTA_QA_CULT_COMP, HHTA_QA_EDIE, HHTA_QA_HAP, HHTA_LORGID, HHTA_READY_TO_BILL, HHTA_DATE_TO_BILL, HHTA_USER_TO_BILL)
          --VALUES ( p_client_id, p_pathway_id, p_pathway_step, p_visitid, l_rec.appt_date /*p_step_date*/, p_plan_id, p_is_pct, 
          --p_assignto, l_rec.appt_date, p_isbillable, p_hubid, p_agencyid, p_id, p_zip, p_census_tract, p_enroll_date, p_enroll_status, p_member_type,
          --p_research_cat_tier1, p_research_cat_tier2 );
          select p_client_id, p_pathway_id, p_pathway_step, p_visitid, l_rec.appt_date /*p_step_date*/, p_plan_id, p_is_pct, 
          p_assignto, l_rec.appt_date, p_isbillable, p_hubid, p_agencyid, p_id, p_zip, p_census_tract, p_enroll_date, p_enroll_status, p_member_type,
          p_research_cat_tier1, p_research_cat_tier2,
          a.START_DATE, a.SERVICE_DATE, a.HH_G_CODE, a.SERVICE_LOC, a.APPROVAL_CODE, a.CONSENT, a.CC_NOTE,
          a.ID_TIER, a.ID_LOC, a.HAP_CODE, a.EXCEPTION, a.INITIALS, a.ASSESSMENTS, a.QA_ADV_DIR, a.QA_CULT_COMP,
          a.QA_EDIE, a.QA_HAP, a.LORGID, a.READY_TO_BILL, a.DATE_TO_BILL, a.USER_TO_BILL
          from dual d
          left join tool_hh_time_activity a on 1=1 and a.id=p_id;

      end if;
    end loop;
  else
    -- Only insert if step not already achieved for this pathway,client, visit, and step
    select count(*) into l_count
    from CLIENT_STEP_STATUS_2
    where pathway_id = p_pathway_id
    and client_id = p_client_id
    and visit_id = p_visitid
    and pathway_step = p_pathway_step;
    
    if l_count = 0 then
    /******************
      insert into 
      CLIENT_STEP_STATUS_2( CLIENT_ID, PATHWAY_ID, PATHWAY_STEP, VISIT_ID, STEP_DATE, PLAN_ID, IS_PCT, ASSIGNTO, COMPLETED_DATE, 
      ISBILLABLE, HUBID, AGENCYID, PK_ID, ZIP_CODE, census_tract, ENROLLMENT_DATE, ENROLL_STATUS,
      HHTA_START_DATE, HHTA_SERVICE_DATE, HHTA_HH_G_CODE, HHTA_SERVICE_LOC, HHTA_APPROVAL_CODE, HHTA_CONSENT, HHTA_CC_NOTE,
      HHTA_ID_TIER, HHTA_ID_LOC, HHTA_HAP_CODE, HHTA_EXCEPTION, HHTA_INITIALS , HHTA_ASSESSMENTS , HHTA_QA_ADV_DIR, 
      HHTA_QA_CULT_COMP, HHTA_QA_EDIE, HHTA_QA_HAP, HHTA_LORGID, HHTA_READY_TO_BILL, HHTA_DATE_TO_BILL, HHTA_USER_TO_BILL)
      --VALUES ( p_client_id, p_pathway_id, p_pathway_step, p_visitid, p_step_date, p_plan_id, p_is_pct, p_assignto, p_completed_date,
      --p_isbillable, p_hubid, p_agencyid, p_id, p_zip, p_census_tract, p_enroll_date, p_enroll_status );
      select p_client_id, p_pathway_id, p_pathway_step, p_visitid, p_step_date, p_plan_id, p_is_pct, p_assignto, p_completed_date,
      p_isbillable, p_hubid, p_agencyid, p_id, p_zip, p_census_tract, p_enroll_date, p_enroll_status,
      a.START_DATE, a.SERVICE_DATE, a.HH_G_CODE, a.SERVICE_LOC, a.APPROVAL_CODE, a.CONSENT, a.CC_NOTE,
      a.ID_TIER, a.ID_LOC, a.HAP_CODE, a.EXCEPTION, a.INITIALS, a.ASSESSMENTS, a.QA_ADV_DIR, a.QA_CULT_COMP,
      a.QA_EDIE, a.QA_HAP, a.LORGID, a.READY_TO_BILL, a.DATE_TO_BILL, a.USER_TO_BILL
      from dual d
      left join tool_hh_time_activity a on 1=1 and a.id=p_id; 
   *****************/   
      insert into 
      CLIENT_STEP_STATUS_2( CLIENT_ID, PATHWAY_ID, PATHWAY_STEP, VISIT_ID, STEP_DATE, PLAN_ID, IS_PCT, ASSIGNTO, COMPLETED_DATE, 
      ISBILLABLE, HUBID, AGENCYID, PK_ID, ZIP_CODE, census_tract, ENROLLMENT_DATE, ENROLL_STATUS, MEMBER_TYPE, 
      RESEARCH_CAT_TIER1, RESEARCH_CAT_TIER2,
      HHTA_START_DATE, HHTA_SERVICE_DATE, HHTA_HH_G_CODE, HHTA_SERVICE_LOC, HHTA_APPROVAL_CODE, HHTA_CONSENT, HHTA_CC_NOTE,
      HHTA_ID_TIER, HHTA_ID_LOC, HHTA_HAP_CODE, HHTA_EXCEPTION, HHTA_INITIALS , HHTA_ASSESSMENTS , HHTA_QA_ADV_DIR, 
      HHTA_QA_CULT_COMP, HHTA_QA_EDIE, HHTA_QA_HAP, HHTA_LORGID, HHTA_READY_TO_BILL, HHTA_DATE_TO_BILL, HHTA_USER_TO_BILL)
      --VALUES ( p_client_id, p_pathway_id, p_pathway_step, p_visitid, p_step_date, p_plan_id, p_is_pct, p_assignto, p_completed_date,
      --p_isbillable, p_hubid, p_agencyid, p_id, p_zip, p_census_tract, p_enroll_date, p_enroll_status, p_member_type, p_research_cat_tier1, p_research_cat_tier2  );
        select p_client_id, p_pathway_id, p_pathway_step, p_visitid, p_step_date, p_plan_id, p_is_pct, p_assignto, p_completed_date,
          p_isbillable, p_hubid, p_agencyid, p_id, p_zip, p_census_tract, p_enroll_date, p_enroll_status, p_member_type, 
          p_research_cat_tier1, p_research_cat_tier2 ,
          a.START_DATE, a.SERVICE_DATE, a.HH_G_CODE, a.SERVICE_LOC, a.APPROVAL_CODE, a.CONSENT, a.CC_NOTE,
          a.ID_TIER, a.ID_LOC, a.HAP_CODE, a.EXCEPTION, a.INITIALS, a.ASSESSMENTS, a.QA_ADV_DIR, a.QA_CULT_COMP,
          a.QA_EDIE, a.QA_HAP, a.LORGID, a.READY_TO_BILL, a.DATE_TO_BILL, a.USER_TO_BILL
          from dual d
          left join tool_hh_time_activity a on 1=1 and a.id=p_id;

    end if;

end if;
        
END ins_client_step_status;

/******************************************************************************/
function generate_column_check_sql( p_id IN VARCHAR2,
                                        p_pathway_table_name IN VARCHAR2,
                                        p_hubid IN HUB_SM_2.HUBID%TYPE,
                                        p_pathway_id IN HUB_SM_2.PATHWAY_ID%TYPE,
                                        p_pathway_step IN HUB_SM_2.PATHWAY_STEP%TYPE,
                                        p_step_group IN HUB_SM_2.STEP_GROUP%TYPE,
                                        p_agencyid IN HUB_SM_2.AGENCYID%TYPE,
                                        p_plan_id IN HUB_SM_2.PLAN_ID%TYPE,
                                        p_is_pct in HUB_SM_2.IS_PCT%TYPE) return varchar2 is
l_sql varchar2(4000);
l_col_cnt int := 1;
l_step_condition HUB_SM_2.step_condition%TYPE;
l_step_sql HUB_SM_2.sql_select%TYPE;
l_priority int;
begin
  if p_pathway_table_name='CLIENTS' THEN
    l_sql := 'SELECT COUNT(*) from '||p_pathway_table_name|| ' c';
    l_sql := l_sql ||' WHERE c.ID = '''||p_id||'''';
  else
    l_sql := 'SELECT COUNT(*) from '||p_pathway_table_name;
    l_sql := l_sql ||' WHERE ID = '''||p_id||'''';
  end if;
  
  dbms_output.put_line('generate_column_check_sql table:' || p_pathway_table_name
  ||' hubid='||p_hubid
  ||' pathway_id='||p_pathway_id
  ||' pathway_step='||p_pathway_step
  ||' pathway_step_group='||p_step_group
  ||' plan_id='||p_plan_id
  ||' agency_id='||p_agencyid
  ||' is_pct='||p_is_pct 
  );
  
  -- we need to match against hub_sm_2 using a pecking order
  -- If we match on all 3 we want that, if we match on hub and agency we want that next, then just hub, and finally on nulls default
  select min(priority) into l_priority from (
                  select case when hubid=p_hubid and agencyid=p_agencyid and plan_id=p_plan_id then 1
                  when hubid=p_hubid and agencyid=p_agencyid and plan_id is null then 2
                  when hubid=p_hubid and agencyid is null and plan_id is null then 3
                  when hubid is null and agencyid is null and plan_id is null then 4 end priority
                  from HUB_SM_2 
                  where (hubid=p_hubid or hubid is null) 
                  and (agencyid=p_agencyid or agencyid is null)
                  and (PLAN_ID = p_plan_id or PLAN_ID is null)
                  and pathway_id=p_pathway_id
                  and pathway_step=p_pathway_step and step_group=p_step_group
                  and is_pct = p_is_pct
                  and ISUNUSED is null
              )t;

  FOR l_rec in (select PATHWAY_COLUMN, step_condition, REPLACE(sql_select, '$$STEP$$', 'TRUNC(' ||TO_CHAR(p_pathway_step) ||')' ) sql_select
                from (
                  select PATHWAY_COLUMN, step_condition, sql_select, 
                  case when hubid=p_hubid and agencyid=p_agencyid and plan_id=p_plan_id then 1
                  when hubid=p_hubid and agencyid=p_agencyid and plan_id is null then 2
                  when hubid=p_hubid and agencyid is null and plan_id is null then 3
                  when hubid is null and agencyid is null and plan_id is null then 4 end priority
                  from HUB_SM_2 
                  where (hubid=p_hubid or hubid is null) 
                  and (agencyid=p_agencyid or agencyid is null)
                  and (PLAN_ID = p_plan_id or PLAN_ID is null)
                  and pathway_id=p_pathway_id
                  and pathway_step=p_pathway_step and step_group=p_step_group
                  and is_pct = p_is_pct
                  and ISUNUSED is null
                  ) where priority = l_priority -- we want to give preference when we match on a hubid and or agency id
                )
  LOOP
    if l_rec.sql_select is not null then
      l_sql := l_sql ||' AND EXISTS('||l_rec.sql_select||')';
    else
        -- l_sql := l_sql || ' AND '||l_rec.pathway_column||' IS NOT NULL';
      l_sql := l_sql || ' AND '||l_rec.pathway_column||' '||l_rec.step_condition;
    end if;
    l_col_cnt := l_col_cnt + 1;
  END LOOP;
  
  -- if the column count is 1 then there were no rows found, return an error to the caller
  if l_col_cnt = 1 then
    l_sql := 'ERROR';
  end if;

  return l_sql;
 
 
end generate_column_check_sql;


/******************************************************************************/
procedure validate_hub_stepmaster is
l_sql varchar2(4000);
l_check_status int;
l_error varchar2(4000);
begin

  
  FOR l_rec in (select * from HUB_SM_2 where isunused is null )
  LOOP
  BEGIN
    if l_rec.pathway_table='CLIENTS' THEN
      l_sql := 'SELECT COUNT(*) from '||l_rec.pathway_table|| ' c';
      l_sql := l_sql ||' WHERE c.ID = (select max(id) from clients)';
    else
      l_sql := 'SELECT COUNT(*) from '||l_rec.pathway_table;
      l_sql := l_sql ||' WHERE ID = (select max(id) from pathway_education)';
    end if;
  
    if l_rec.sql_select is not null then
      l_sql := l_sql ||' AND EXISTS('||l_rec.sql_select||')';
    else
        -- l_sql := l_sql || ' AND '||l_rec.pathway_column||' IS NOT NULL';
      l_sql := l_sql || ' AND '||l_rec.pathway_column||' '||l_rec.step_condition;
    end if;
    
     EXECUTE IMMEDIATE l_sql INTO l_check_status;
  EXCEPTION
  WHEN OTHERS THEN
  BEGIN
    l_error := substr( SQLERRM, 1, 4000);
    dbms_output.put_line('PATHWAY_ID:' || l_rec.pathway_id
  ||' pathway_table='||l_rec.pathway_table
  ||' pathway_step='||l_rec.pathway_step
  ||' pathway_step_group='||l_rec.step_group
  ||' hub_id='||l_rec.hubid
  ||' plan_id='||l_rec.plan_id
  ||' agency_id='||l_rec.agencyid
  ||' is_pct='||l_rec.is_pct 
  ||' SQL='||l_sql
  ||' ERROR='||l_error
  );
  insert into hub_step_master_validate(pathway_id, pathway_table, pathway_step, step_group,
  hubid, agency_id, plan_id, is_pct, sql_statement, sql_error )
  VALUES(l_rec.pathway_id, l_rec.pathway_table, l_rec.pathway_step, l_rec.step_group, 
  l_rec.hubid, l_rec.agencyid, l_rec.plan_id, l_rec.is_pct, l_sql, l_error);
 END;
  END;
  END LOOP;
  
  COMMIT;
 
 
end validate_hub_stepmaster;


/*************************************/ 
function get_completed_date( p_pathway_id in HUB_STEP_CLOSE_2.PATHWAY_ID%type, p_id in VARCHAR2, p_hubid in HUB_STEP_CLOSE_2.HUBID%type, 
                                   p_pathway_step IN HUB_STEP_CLOSE_2.PATHWAY_STEP%type,  p_step_group IN HUB_STEP_CLOSE_2.STEP_GROUP%type,
                                   p_is_pct HUB_SM_2.IS_PCT%TYPE,
                                   p_appt_dates_logic_flag HUB_SM_2.appt_dates_logic_flag%TYPE,
                                   p_appt_type_token HUB_SM_2.appt_type_token%TYPE,
                                   p_visit_id client_pathway_appt_dates.CLIENT_PATHWAY_VISITID%TYPE) 
                                   RETURN DATE IS
l_date DATE;
l_table_name names_pathways.pathway_table%TYPE;
l_col_name HUB_STEP_CLOSE_2.table_column%TYPE;
l_sql varchar2(512);
l_key_string varchar2(512);
BEGIN
  if p_appt_dates_logic_flag='Y' then
    select max(appt_date)
    into l_date
    from client_pathway_appt_dates
    where CLIENT_PATHWAY_VISITID=p_visit_id
    and appt_type like '%'||p_appt_type_token||'%';
    
  else
    select pathway_table INTO l_table_name from names_pathways where pathway_id = p_pathway_id;
    
    select table_column into l_col_name
    --from HUB_STEPCLOSE
    from HUB_STEP_CLOSE_2
    where pathway_id = p_pathway_id
    and is_pct = p_is_pct
    and pathway_step = TRUNC(p_pathway_step)
    and step_group = p_step_group;
    --and hubid = p_hubid  _ALL HUBDS are null in HUB_STEPCLOSE
    
    l_sql := 'SELECT '||l_col_name||' FROM '|| l_table_name||' WHERE ID = '''|| p_id ||'''';
    dbms_output.put_line( l_sql );
    EXECUTE IMMEDIATE l_sql INTO l_date;
  end if;
  return l_date;
EXCEPTION
WHEN OTHERS THEN
    l_key_string:='pathway_id='||p_pathway_id||' pct='||p_is_pct||' step='||p_pathway_step||' group='||p_step_group;
    --ins_pkg_pathway_step_error( p_table_name, p_pathway_step, p_error_message , p_pathway_table_id, p_visitid, p_ispct )
    if l_sql is null then
      ins_pkg_pathway_step_error( l_table_name, p_pathway_step, 'get_completed_date: '||l_key_string||' : '||SQLERRM, p_id, null, p_is_pct);
    else
      ins_pkg_pathway_step_error( l_table_name, p_pathway_step, 'get_completed_date: '||l_sql||':'||SQLERRM, p_id, null, p_is_pct);
    end if;
    
    
    DBMS_OUTPUT.put_line('Exception '||SQLERRM);
  return null;
END get_completed_date;

/*****************************************************************/
procedure test_pathway_step_processing(p_pathway_table in varchar2, p_id varchar2) IS
l_status boolean;
l_step HUB_SM_2.pathway_step%TYPE;
begin
  l_step := pathway_step_processing(p_pathway_table, p_id, l_status ); 
  commit;
end;
/*****************************************************************/
procedure process_client_enrollment_q AS
l_processed_date date := SYSDATE;
begin
    begin
      update queue_steps_client_enrollment set attempted_date = l_processed_date where attempted_date is null;
      commit;
    
      /* Adding to re-direct enrollment checks for the appropriate initial checklist */
      INSERT INTO queue_pathway_steps(pathway_table_name,id,client_enrollment_entry_flag,client_id)
      select src.table_name, ids.id,'Y', src.client_id
      from queue_steps_client_enrollment q
      join v_mr_checklists_source src on q.client_id = src.client_id
      join v_mr_initial_checklist_dtl_ids ids on ids.client_id = src.client_id and ids.detail_table=src.table_name and ids.visit_id = src.client_checklist_visitid
      where 
      q.attempted_date = l_processed_date and q.processed_date is null
      and not exists (
        SELECT 1 FROM queue_pathway_steps q where q.processed_date is null and pathway_table_name=src.table_name and q.ID = ids.id 
      );
      -- Mark records as processed
      update queue_steps_client_enrollment set processed_date = SYSDATE where attempted_date = l_processed_date and processed_date is null;
    
      COMMIT;
    
      EXCEPTION
      WHEN OTHERS THEN
        rollback;
        ins_pkg_pathway_step_error( 'queue_steps_client_enrollment', null, 'process_client_enrollment_q: '||SQLERRM, null, null, null);
      END;

end process_client_enrollment_q;
/*****************************************************************/
function pathway_step_processing(p_pathway_table in varchar2, p_id varchar2, p_status in out boolean) return HUB_SM_2.pathway_step%TYPE is
l_hubid  HUB_SM_2.HUBID%TYPE;
l_agency_id HUB_SM_2.AGENCYID%TYPE;
l_pathway_id HUB_SM_2.PATHWAY_ID%TYPE;
l_client_id clients_pathways.CLIENTID%TYPE;
l_pathway_step HUB_SM_2.pathway_step%TYPE;
l_step_label HUB_SM_2.step_label%TYPE;
l_sql varchar(4000);
l_status varchar(20);
l_check_status int;
l_max_final_pathway_step HUB_SM_2.pathway_step%TYPE:=0;
l_max_final_completed_status HUB_SM_2.step_label%TYPE;
l_max_normal_pathway_step HUB_SM_2.pathway_step%TYPE:=0;
l_visitid VARCHAR2(30);
l_completed_date DATE;
l_final_completed_date DATE;
--l_record_updated_date DATE;
l_record_created_date DATE;
l_current_step number;
l_ishistory clients_pathways.ishistory%TYPE;
l_query_tag varchar2(80);
l_count int;
l_plan_id HUB_SM_2.plan_id%TYPE;
l_is_pct HUB_SM_2.IS_PCT%TYPE;
l_assignto clients_pathways.ASSIGNTO%TYPE;
l_checklist_id client_checklists.checklist_id%TYPE;
l_tool_id clients_tools.tool_id%TYPE;
c_id    ref_cur;
l_checklist_visit_date date;
l_zipcode clients.zip_code%TYPE;
l_census_tract clients.census_tract%TYPE;
l_enrollment_date clients.enrollment_date%TYPE;
l_enrollment_status clients.enroll_status%TYPE;
l_member_type client_step_status_2.member_type%TYPE;
l_research_cat_tier1 clients_pathways.research_cat_tier1%TYPE;
l_research_cat_tier2 clients_pathways.research_cat_tier2%TYPE;
BEGIN
  dbms_output.put_line('Inside pathway_step_processing');
  l_sql := 'select count(*) from '||p_pathway_table;--||' where ID ='''||p_id||'''';
  EXECUTE IMMEDIATE l_sql INTO l_count;
  --ins_pkg_pathway_step_error( 'PATHWAY_EDUCATION', l_current_step, 'COUNT IS '||l_count);
  l_query_tag := 'Getting Hubid, pathway_id, visit_id for ID='||p_id;
  /************************
  l_sql := 'select c.hubid, c.care_coordination_agency, NVL(NVL(cp.clientid, cc.client_id), ct.clientid) client_id ' 
  ||' cp.pathway_id, pe.visit_id, convert_string_to_date(pe.recordcreateddate),'
  ||' cp.assignto, '
  ||' case when cp.clientid is not null then ''P'' else null end is_pct'
  --into l_hubid, l_client_id, l_pathway_id, l_visitid, l_record_updated_date
  ||' from '||p_pathway_table||' pe'
  ||' join clients_pathways cp on pe.visit_id = cp.client_pathway_visitid'
  ||' join clients c on c.client_id = cp.clientid'
  ||' where pe.id = '''|| p_id ||'''';
    hubid, care_coordination_agency, client_id, pathway_id, visit_id, record_createddate, assignto, is_pct
    hubid, clientid, pathway_id, visit_id recordipdateddate, assignto, checklist_id, tool_id, is_pct, care_coordination_agency, hub_plan_id
  ******************/
  if p_pathway_table = 'CLIENTS' THEN
    l_sql := ' select c.hubid, c.client_id, 6 pathway_id, c.client_id VISIT_ID, convert_string_to_date(c.recoredupdateddate),'
    ||' c.assignto, null checklist_id, null tool_id, ''A'' is_pct, c.care_coordination_agency, ci.hub_plan_id, null checklist_visit_date,'
    ||' c.zip_code, c.census_tract, c.enrollment_date, c.enroll_status,'
    ||' case when upper(c.client_type)=''PEDIATRIC'' then ''P'' when upper(c.client_type)=''ADULT'' then ''A'' '
    ||' when upper(c.client_type)=''MATERNAL'' then ''M'' when upper(c.client_type)=''SENIOR'' then ''S'' else null end member_type,'
    ||' null research_cat_tier1, null research_cat_tier2'
    ||' from clients c'
    --||' left outer join v_mr_client_insurance_bytype ci on ci.client_id = c.client_id'
    ||' left outer join v_client_insurance_plans ci on ci.client_id = c.client_id'
    ||' where c.id = :P_ID';
  else
    l_sql := 'select c.hubid, NVL(NVL(cp.clientid, cc.client_id), ct.clientid) client_id, cp.pathway_id, '
    ||'  pct.visit_id, convert_string_to_date(pct.recoredupdateddate), NVL(NVL(cp.assignto,cc.assignto), ct.assignto) assignto,'
    ||' cc.checklist_id, ct.tool_id,'
    --||' case when cp.clientid is not null then ''P'' '
    --||' when cc.client_id is not null then ''C'' '
    --||' when ct.clientid is not null then ''T'' '
    ||' CASE WHEN '''||UPPER(p_pathway_table)||''' like ''T%'' then ''T'' '
    ||' WHEN '''||UPPER(p_pathway_table)||''' like ''C%'' then ''C'' '
    ||' WHEN '''||UPPER(p_pathway_table)||''' like ''P%'' OR '''||UPPER(p_pathway_table)||''' like ''M%'' then ''P'' '
    ||' else null end is_pct,'
    ||' c.care_coordination_agency,'
    ||' ci.hub_plan_id, cc.visit_date checklist_visit_date,'
    ||' c.zip_code, c.census_tract, c.enrollment_date, c.enroll_status,'
    ||' case when upper(c.client_type)=''PEDIATRIC'' then ''P'' when upper(c.client_type)=''ADULT'' then ''A'' '
    ||' when upper(c.client_type)=''MATERNAL'' then ''M'' when upper(c.client_type)=''SENIOR'' then ''S'' else null end member_type,'
    ||' cp.research_cat_tier1, cp.research_cat_tier2'
    --into l_hubid, l_client_id, l_pathway_id, l_visitid, l_record_updated_date
    ||' from '||p_pathway_table||' pct'
    ||' left outer join clients_pathways cp on pct.visit_id = cp.client_pathway_visitid'
    ||' left outer join client_checklists cc on pct.visit_id = cc.client_checklist_visitid'
    ||' left outer join clients_tools ct on pct.visit_id = ct.client_tool_visitid'
    ||' join clients c on c.client_id = NVL(NVL(cp.clientid, cc.client_id), ct.clientid)'
    --||' left outer join v_mr_client_insurance_bytype ci on ci.client_id = c.client_id'
    ||' left outer join v_client_insurance_plans ci on ci.client_id = c.client_id'
    ||' where pct.id = :P_ID';
  end if;

  --ins_pkg_pathway_step_error( 'PATHWAY_EDUCATION', l_current_step, l_sql);
  
  dbms_output.put_line(l_sql);
  OPEN c_id for l_sql using p_id;
  LOOP
    FETCH c_id INTO l_hubid, l_client_id, l_pathway_id, l_visitid, l_record_created_date, l_assignto,
                                l_checklist_id, l_tool_id, l_is_pct, l_agency_id, l_plan_id, l_checklist_visit_date,
                                l_zipcode, l_census_tract, l_enrollment_date, l_enrollment_status, l_member_type,
                                l_research_cat_tier1, l_research_cat_tier2;
    EXIT WHEN c_id%NOTFOUND;
  --EXECUTE IMMEDIATE l_sql INTO l_hubid, l_agency_id, l_client_id, l_pathway_id, l_visitid, l_record_created_date, l_assignto, l_is_pct;
  --EXECUTE IMMEDIATE l_sql INTO l_hubid, l_client_id, l_pathway_id, l_visitid, l_record_created_date, l_assignto,
  --                              l_checklist_id, l_tool_id, l_is_pct, l_agency_id, l_plan_id;
    dbms_output.put_line('HubID:'||l_hubid||' ClientID:'||l_client_id||' pathway_id:'||NVL(NVL(l_pathway_id,l_checklist_id), l_tool_id) 
                              ||' agency_id:'||l_agency_id||' plan_id:'||l_plan_id
                              ||' zip:'||l_zipcode||' census_tract:'|| l_census_tract||' enroll date:'||TO_CHAR(l_enrollment_date,'YYYYMMDD')
                              ||' enroll status:'||l_enrollment_status) ;                  
   /****
    if l_record_updated_date is null then
        -- insert into error table???
        l_completed_date := SYSDATE;
    else
       l_completed_date := l_record_updated_date;
    end if;
    */
    -- get the list of steps to iterate over
    for l_rec in (select pathway_step, max(appt_dates_logic_flag) appt_dates_logic_flag, max(appt_type_token) appt_type_token,
                  max(completion_step_flag) completion_step_flag, max(final_step_flag) final_step_flag
                          from HUB_SM_2
                          where (hubid=l_hubid or hubid is null) 
                          and (agencyid=l_agency_id or agencyid is null)
                          and (plan_id=l_plan_id or plan_id is null) 
                          and pathway_id=NVL(NVL(l_pathway_id, l_checklist_id), l_tool_id)
                          and is_pct = l_is_pct
                          and ISUNUSED IS NULL -- FILTER OUT STEPS THAT MAY BE MARKED AS UNUSED
                  group by pathway_step
                  order by pathway_step)
    loop
      l_current_step := l_rec.pathway_step;
      --l_final_completed_date := null;
      dbms_output.put_line('');
      dbms_output.put_line('HubID:'||l_hubid||' ClientID:'||l_client_id||' Step # is '||l_rec.pathway_step);
      l_query_tag := 'Opening stepgroup cursor';
      for l_stepgroup_rec in (select distinct step_group, step_label, plan_id, is_pct, isbillable
                          from HUB_SM_2
                          where (hubid=l_hubid or hubid is null) 
                          and (agencyid=l_agency_id or agencyid is null)
                          and (plan_id=l_plan_id or plan_id is null) 
                          and pathway_id=NVL(NVL(l_pathway_id, l_checklist_id), l_tool_id) 
                          and pathway_step=l_rec.pathway_step
                          and is_pct = l_is_pct
                          and ISUNUSED IS NULL -- FILTER OUT STEPS THAT MAY BE MARKED AS UNUSED
                          order by step_group
                          )
      LOOP
      BEGIN
        l_plan_id := l_stepgroup_rec.plan_id;
        l_is_pct := l_stepgroup_rec.is_pct;
        dbms_output.put_line('      STEP GROUP: '||l_stepgroup_rec.step_group);
        l_query_tag := 'Calling generate_column_nullcheck_sql';
        l_sql := generate_column_check_sql( p_id, 
                                        p_pathway_table,
                                        l_hubid,
                                        NVL(NVL(l_pathway_id, l_checklist_id), l_tool_id),
                                        l_rec.pathway_step,
                                        l_stepgroup_rec.step_group,
                                        l_agency_id,
                                        l_plan_id,
                                        l_is_pct);
        dbms_output.put_line(l_sql);
        if l_sql != 'ERROR' THEN
          -- THIS RUNS THE VALIDATION CHECK
          EXECUTE IMMEDIATE l_sql INTO l_check_status;
          dbms_output.put_line('STATUS IS '||l_check_status);
          
          --if l_status = 'SUCCESS' then
          if l_check_status > 0 then
            -- 20-49 and 191, 292 are the only steps that denote completion
            --if l_rec.pathway_step between 20 and 49 or l_rec.pathway_step in (190,191,192,290,291,292) then 
            if l_rec.completion_step_flag = 'Y' then
              -- This is a final step check
              --if l_rec.pathway_step between 20 and 49 then
              if l_rec.final_step_flag = 'Y' then
                l_max_final_pathway_step := l_rec.pathway_step;
                l_max_final_completed_status := l_stepgroup_rec.step_label;
                l_ishistory := 'S';
              end if;
              l_step_label := l_stepgroup_rec.step_label;
              --l_ishistory := 'S';  --???? need to move this?
              -- Query HUB_STEPCLOSE to get the name of the column that contains the completed_date that
              -- neeed to be used to update the client_pathways record
              -- maybe create a function and pass it:
              -- the id of the pathway record, the pathway_step, step_group, hub, the pathway_detail  table name
              if l_is_pct = 'P' THEN
                -- Completion Date only applies to Pathways!
                l_final_completed_date := get_completed_date(l_pathway_id, p_id, l_hubid, l_rec.pathway_step, 
                                                        l_stepgroup_rec.step_group, l_is_pct, l_rec.appt_dates_logic_flag,
                                                        l_rec.appt_type_token, l_visitid);
              else
                l_final_completed_date := l_record_created_date;
              end if;
              if l_is_pct = 'C' THEN
                l_final_completed_date := l_checklist_visit_date;
              end if;
              ins_client_step_status( l_client_id, NVL(NVL(l_pathway_id, l_checklist_id), l_tool_id), l_rec.pathway_step,
                              l_visitid, l_record_created_date, l_plan_id, l_is_pct, l_assignto, 
                              l_final_completed_date, l_stepgroup_rec.isbillable, l_hubid, l_agency_id,
                              l_rec.appt_dates_logic_flag, p_id, 
                              l_zipcode, l_census_tract, l_enrollment_date, l_enrollment_status, l_member_type, l_rec.appt_type_token,
                              l_research_cat_tier1, l_research_cat_tier2);
            else -- This is either a normal step check (less than 20) or an insurance code step
              if l_rec.pathway_step < 20 then -- normal steps should always be less than or equal 20
                l_max_normal_pathway_step := l_rec.pathway_step;
              end if;
              l_query_tag := 'Inserting into client_step_status_2';
              -- If not a normal step try and look up the completed date as designated by hub_step_close
              if l_rec.pathway_step > 20 then
                 if l_is_pct = 'P' THEN
                -- Completion Date only applies to Pathways!
                    l_completed_date := get_completed_date( l_pathway_id, p_id, l_hubid, l_rec.pathway_step, 
                                          l_stepgroup_rec.step_group, l_is_pct, l_rec.appt_dates_logic_flag,
                                          l_rec.appt_type_token, l_visitid);
                 else
                    l_completed_date := l_record_created_date;
                end if;
              
                
              end if;
              
              -- IF completed_date is null then use l_record_created_date which is the recoredupdated date from the pathway detail record
              if l_completed_date is null then
                l_completed_date := l_record_created_date;
              end if;
                
              dbms_output.put_line(l_query_tag);
              if l_is_pct = 'C' THEN
                l_completed_date := l_checklist_visit_date;
              end if;
              -- any step achieved need a status record
              ins_client_step_status( l_client_id,NVL(NVL(l_pathway_id, l_checklist_id), l_tool_id), l_rec.pathway_step,
                                    l_visitid, l_record_created_date, l_plan_id, l_is_pct, l_assignto, l_completed_date, 
                                    l_stepgroup_rec.isbillable, l_hubid, l_agency_id, l_rec.appt_dates_logic_flag, p_id,
                                    l_zipcode, l_census_tract, l_enrollment_date, l_enrollment_status, l_member_type, l_rec.appt_type_token,
                                    l_research_cat_tier1, l_research_cat_tier2 );
            end if;
          end if;  -- end if path step check passed
        else
          p_status := FALSE;
        end if; -- Make sure the SQL was able to be generated
      EXCEPTION
      WHEN OTHERS THEN
        p_status := false;
        ins_pkg_pathway_step_error( p_pathway_table, l_current_step, l_query_tag||':'||SQLERRM, p_id, l_visitid, l_is_pct);
        DBMS_OUTPUT.put_line('Exception '||SQLERRM);
      END;
      END LOOP;
  end loop;
  END LOOP;
  close c_id;
  
  DBMS_OUTPUT.put_line('MAX FINAL STEP FOR '||p_id||' IS '||l_max_final_pathway_step);
  DBMS_OUTPUT.put_line('MAX NORMAL STEP FOR '||p_id||' IS '||l_max_normal_pathway_step);
  
  --if l_max_final_pathway_step > 0 then
  --  l_query_tag := 'Inserting into client_step_status';
      -- any step achieved
      --insert into CLIENT_STEP_STATUS_2( CLIENT_ID, PATHWAY_ID, PATHWAY_STEP, VISIT_ID, STEP_DATE)
      --  VALUES ( l_client_id, l_pathway_id, l_max_final_pathway_step, l_visitid, l_completed_date);
   --   ins_client_step_status_2( l_client_id,l_pathway_id, l_max_final_pathway_step,
    --                          l_visitid, l_final_completed_date, l_plan_id, l_is_pct, l_assignto );
  --end if;
  
  if l_max_final_pathway_step = 0 then
    l_max_final_pathway_step := null;
  end if;
 
  dbms_output.put_line('l_final_completed_date is '||to_char(l_final_completed_date,'YYYYMMDD'));
  
  if l_is_pct = 'P' THEN
     
  l_query_tag := 'Updating clients_pathways';
      dbms_output.put_line('Updating clients_pathways');
      update clients_pathways
      set steps_completed = l_max_normal_pathway_step,
      completed_step = CASE WHEN l_max_final_pathway_step is not null then l_max_final_pathway_step else completed_step end,
      completed_status = CASE WHEN l_max_final_completed_status is not null then l_max_final_completed_status else completed_status end,
      ishistory = CASE WHEN l_ishistory is not null then l_ishistory else ishistory end,
      completed_date = CASE WHEN l_final_completed_date is not null then l_final_completed_date else completed_date end
    where client_pathway_visitid = l_visitid;
    -- set completed_date only if l_max_normal_pathway_step is set
    dbms_output.put_line('After Updating clients_pathways');
  end if;
  
   if l_is_pct = 'T' THEN
     
      l_query_tag := 'Updating clients_tools';
      dbms_output.put_line('Updating clients_tools');
      
      update clients_tools
      set
      ishistory = CASE WHEN l_ishistory is not null then l_ishistory else ishistory end
      where client_tool_visitid = l_visitid;
       -- set completed_date only if l_max_normal_pathway_step is set
      dbms_output.put_line('After Updating clients_tools');
  end if;
  
    --COMMIT;
  return l_max_normal_pathway_step;

  
  EXCEPTION
  WHEN OTHERS THEN
    --rollback;
     p_status := false;
     ins_pkg_pathway_step_error( p_pathway_table, l_current_step, l_query_tag||':'||SQLERRM, p_id, null, null);
    DBMS_OUTPUT.put_line('Exception '||SQLERRM);
    --RAISE_APPLICATION_ERROR(-20000, 'Exception: '||SQLERRM);
    return null;

  
  end pathway_step_processing;
  
/*****************************************************************/
procedure client_code_processing  is
l_count int;
l_step_status_processed_cnt int:=0;
l_plan_code CLIENT_CODE_STATUS_2.plan_code%type;
l_plan_dollar_amt CLIENT_CODE_STATUS_2.PLAN_DOLLAR_AMT%TYPE;
l_ccs_code CLIENT_CODE_STATUS_2.CCS_CODE%TYPE;
l_ccs_rvu CLIENT_CODE_STATUS_2.CCS_RVU_AMT%TYPE;
l_plan_rvu_amt CLIENT_CODE_STATUS_2.PLAN_RVU_AMT%TYPE;
l_rvu_value HUB_PLANRVU.RVU_DEFAULT%TYPE;
l_code_amt CLIENT_CODE_STATUS_2.CODE_AMOUNT%TYPE;
l_step_rec_status boolean;
l_plan_sub_code HUB_CM_2.plan_sub_code%TYPE;
l_exception_logged boolean:=FALSE;
l_description HUB_CM_2.description%TYPE;
l_sub_desc HUB_CM_2.sub_desc%TYPE;
l_signer_username CLIENT_CODE_STATUS_2.signer_username%TYPE;
l_signer_doc_date CLIENT_CODE_STATUS_2.signer_doc_date%TYPE;
l_signer_isunbillable signer_detail.is_unbillable%TYPE; 
l_sub_desc_other CLIENT_CODE_STATUS_2.sub_desc_other%TYPE;
l_code_check_count int;
l_is_unbillable HUB_CM_2.IS_UNBILLABLE%TYPE;
l_is_unbill_date HUB_CM_2.is_unbill_date%TYPE;
l_is_unbill_username HUB_CM_2.is_unbill_username %TYPE;
l_is_unpayable HUB_CM_2.IS_UNPAYABLE%TYPE;
l_is_unpay_date HUB_CM_2.is_unpay_date%TYPE;
l_is_unpay_username HUB_CM_2.is_unpay_username %TYPE;

BEGIN
BEGIN
  dbms_output.put_line('Inside client_code_processing');
 
  -- get the list of steps to iterate over
  for l_rec in (select css.status_id, css.client_id, css.pathway_id, css.pathway_step, css.visit_id, css.step_date, 
                case when ci.hub_plan_id between 900 and 999 then null else ci.hub_plan_id end plan_id_4_lookup, -- plan_id's 900-999 are ot real plans
                ci.hub_plan_id plan_id,
                css.assignto, 
                case when ci.hub_plan_id between 900 and 999 then null else css.hubid end hubid_4_lookup, -- plan_id's 900-999 are ot real plans
                css.hubid,
                case when ci.hub_plan_id between 900 and 999 then null else css.agencyid end agencyid_4_lookup, -- plan_id's 900-999 are ot real plans
                css.agencyid,
                case when upper(c.client_type)='PEDIATRIC' then 'P'
                     when upper(c.client_type)='ADULT' then 'A'
                     when upper(c.client_type)='PREGNANT' then 'R'
                     when upper(c.client_type)='MATERNAL' then 'M'
                     when upper(c.client_type)='SENIOR' then 'S'
                else null 
                end member_type,
                css.is_pct,
                css.completed_date,
                ci.insurance_number,
                c.riskq_status,
                ci.insurance_type,
                css.pk_id,
                ci.PLAN_WTRFL_LEVEL,
                css.ZIP_CODE,
                css.census_tract,
                css.ENROLLMENT_DATE,
                css.ENROLL_STATUS,
                css.RESEARCH_CAT_TIER1,
                css.RESEARCH_CAT_TIER2,
                css.HHTA_START_DATE,
                css.HHTA_SERVICE_DATE,
                css.HHTA_HH_G_CODE ,
                css.HHTA_SERVICE_LOC ,
                css.HHTA_APPROVAL_CODE,
                css.HHTA_CONSENT,
                css.HHTA_CC_NOTE,
                css.HHTA_ID_TIER,
                css.HHTA_ID_LOC,
                css.HHTA_HAP_CODE,
                css.HHTA_EXCEPTION,
                css.HHTA_INITIALS ,
                css.HHTA_ASSESSMENTS ,
                css.HHTA_QA_ADV_DIR,
                css.HHTA_QA_CULT_COMP,
                css.HHTA_QA_EDIE,
                css.HHTA_QA_HAP,
                css.HHTA_LORGID,
                css.HHTA_READY_TO_BILL,
                css.HHTA_DATE_TO_BILL,
                css.HHTA_USER_TO_BILL
                from client_step_status_2 css 
                --left outer join v_mr_client_insurance_bytype ci on css.client_id = ci.client_id --and ci.hub_plan_id is not null
                left outer join v_client_insurance_plans ci on ci.client_id = css.client_id
                join clients c on c.client_id = css.client_id
                where css.is_coded ='N'
                and css.isbillable='Y'
                and css.code_attempted_date is null
                and css.ismarked is null
                -- need to order by the waterfall level so we can avoid double submissions
                order by ci.PLAN_WTRFL_LEVEL)
  LOOP
  BEGIN
      l_exception_logged := FALSE;
      update client_step_status_2 set code_attempted_date = SYSDATE where status_id=l_rec.status_id;
      commit; -- Added on 1/27/2017 to address situation where records just keep getting prcessed over and over
              -- Need to see if this causes probems with the cursor
      l_step_rec_status := true;
      dbms_output.put_line('');
      dbms_output.put_line('Status ID:'||l_rec.status_id||' client_id:'||l_rec.client_id||' pathway_id:'||l_rec.pathway_id
      ||' pathway_step:'||l_rec.pathway_step||' hubid:'||l_rec.hubid||' agencyid:'||l_rec.agencyid
      ||' member_type:' ||l_rec.member_type||'Plan Id:'||l_rec.plan_id );
      -- Look up signer_doc_date and signer_username for this client and step date
      BEGIN
          select signer_doc_date,  username, is_unbillable
          into l_signer_doc_date, l_signer_username, l_signer_isunbillable
          from (
            select client_id, signer_doc_date,  username, is_unbillable, row_number() over(partition by client_id order by signer_doc_date) rn
            from signer_detail
          where client_id=l_rec.client_id and document_id=l_rec.visit_id and signer_doc_date>=TRUNC(l_rec.step_date)
      ) where rn=1;
      EXCEPTION
      WHEN OTHERS THEN
        /* No error if there was a retrieval problem so commented out the error */
        --ins_pkg_pathway_code_error(l_rec.status_id, l_rec.client_id, l_rec.pathway_id, l_rec.pathway_step, l_rec.visit_id, 
        --                  l_rec.step_date, 'SIGNER_DETAIL Lookup failed for'
        --                    ||' client_id='||l_rec.client_id||' step_date='||TO_CHAR(l_rec.step_date, 'MM/DD/YYYY'),
        --                    l_rec.is_pct);
        l_signer_username := null;
        l_signer_doc_date := null;
        l_signer_isunbillable := null;
      END;
      -- INITIALIZE l_sub_desc_other
      l_sub_desc_other := null;
      -- Need to pull in sub_desc_other for a descrete set of pathways that we need additional information for on the invoice
      BEGIN
        -- MEDICAL REFERREAL
        if l_rec.pathway_id = 11 and l_rec.pathway_step in (1101.099, 91101.099) and l_rec.is_pct='P' then
            select referral_type_other into l_sub_desc_other from pathway_medical_referral where ID = l_rec.pk_id;
        end if;
        if l_rec.pathway_id = 11 and l_rec.pathway_step in (1101.999, 91101.999) and l_rec.is_pct='P' then
            select referral_type into l_sub_desc_other from pathway_medical_referral where ID = l_rec.pk_id;
        end if;
         if l_rec.pathway_id = 11 and l_rec.pathway_step in (1101.006, 91101.006) and l_rec.is_pct='P' then
            select REFERRAL_TYPE_SPECIAL_MED_CARE into l_sub_desc_other from pathway_medical_referral where ID = l_rec.pk_id;
        end if;
        
        -- SOCIAL SERVICE REF
        if l_rec.pathway_id = 14 and l_rec.pathway_step in (1401.099, 91401.099) and l_rec.is_pct='P' then
            select code_number_other into l_sub_desc_other from pathway_social_service_ref where ID = l_rec.pk_id;
        end if;
        if l_rec.pathway_id = 14 and l_rec.pathway_step in (1401.999, 91401.999) and l_rec.is_pct='P' then
            select code_number into l_sub_desc_other from pathway_social_service_ref where ID = l_rec.pk_id;
        end if;
        -- Education
        if l_rec.pathway_id = 17 and l_rec.pathway_step in (187, 90187) and l_rec.is_pct='P' then
           select module_type||' -'||module_other into l_sub_desc_other 
           from clients_pathways where CLIENT_PATHWAY_VISITID = l_rec.visit_id and pathway_id=17;
        end if;  
                                      
      EXCEPTION
      WHEN OTHERS THEN
        ins_pkg_pathway_code_error(l_rec.status_id, l_rec.client_id, l_rec.pathway_id, l_rec.pathway_step, l_rec.visit_id, 
                          l_rec.step_date, 'SUB_DESC_OTHER Lookup failed for'
                            ||' ID='||l_rec.pk_id||' visit_id='||l_rec.visit_id,
                            l_rec.is_pct);
        l_sub_desc_other := null;
      END;
      
          -- CHANGE THIS CODE TO DO THE REQUIRED LOOKUPS 
          -- MATCH ON THE HUBID/AGENCID FIRST AND IF NO MATCH LOOK FOR NULL/NULL
          BEGIN
            dbms_output.put_line( 'Querying HUB_CM_2 with hubid/agencyid/plan_id');
            -- FIRST TRY AND MATCH ON THE CLIENT HUBID/AGENCYID
            select plan_code, plan_dollar_amt, plan_rvu_amt, ccs_code, ccs_rvu, plan_rvu_amt, plan_sub_code,
            description, sub_desc, IS_UNBILLABLE, IS_UNPAYABLE
            INTO l_plan_code, l_plan_dollar_amt, l_plan_rvu_amt, l_ccs_code, l_ccs_rvu, l_plan_rvu_amt, l_plan_sub_code, 
            l_description, l_sub_desc, l_is_unbillable, l_is_unpayable
            from HUB_CM_2
            where hubid = l_rec.hubid_4_lookup
            and agencyid = l_rec.agencyid_4_lookup
            and plan_id=l_rec.plan_id_4_lookup
            and pathway_id=l_rec.pathway_id 
            and pathway_step=l_rec.pathway_step
            and member_type=l_rec.member_type
            and is_pct=l_rec.is_pct;
          EXCEPTION
          WHEN NO_DATA_FOUND THEN
            BEGIN
              dbms_output.put_line( 'Querying HUB_CM_2 with hubid/agencyid=NULL');
              -- SAME QUERY AS ABOVE but now with null HUBID and provided AGENCYID
              select plan_code, plan_dollar_amt, plan_rvu_amt, ccs_code, ccs_rvu, plan_rvu_amt, plan_sub_code, 
              description, sub_desc, IS_UNBILLABLE, IS_UNPAYABLE
              INTO l_plan_code, l_plan_dollar_amt, l_plan_rvu_amt, l_ccs_code, l_ccs_rvu, l_plan_rvu_amt, l_plan_sub_code, 
              l_description, l_sub_desc, l_is_unbillable, l_is_unpayable
              from HUB_CM_2
              where hubid = l_rec.hubid_4_lookup
              and agencyid is null
              and plan_id=l_rec.plan_id_4_lookup
              and pathway_id=l_rec.pathway_id 
              and pathway_step=l_rec.pathway_step
              and member_type=l_rec.member_type
              and is_pct=l_rec.is_pct;
            EXCEPTION
              -- NOW LOOK FOR HUBID is null and AGENCYID is null (defauts)
              WHEN NO_DATA_FOUND THEN
              BEGIN
                dbms_output.put_line( 'Querying HUB_CM_2 with hubid=null/agencyid=NULL');
                select plan_code, plan_dollar_amt, plan_rvu_amt, ccs_code, ccs_rvu, plan_rvu_amt, plan_sub_code,
                description, sub_desc, IS_UNBILLABLE, IS_UNPAYABLE
                INTO l_plan_code, l_plan_dollar_amt, l_plan_rvu_amt, l_ccs_code, l_ccs_rvu, l_plan_rvu_amt, l_plan_sub_code, 
                l_description, l_sub_desc, l_is_unbillable, l_is_unpayable
                from HUB_CM_2
                where hubid is null
                and agencyid is null
                and plan_id=l_rec.plan_id_4_lookup
                and pathway_id=l_rec.pathway_id 
                and pathway_step=l_rec.pathway_step
                and member_type=l_rec.member_type
                 and is_pct=l_rec.is_pct;
                
                EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                  BEGIN
                    dbms_output.put_line( 'Querying HUB_CM_2 with hubid=null/agencyid=NULL/plan_id=NULL');
                    select plan_code, plan_dollar_amt, plan_rvu_amt, ccs_code, ccs_rvu, plan_rvu_amt, plan_sub_code,
                    description, sub_desc, IS_UNBILLABLE, IS_UNPAYABLE
                    INTO l_plan_code, l_plan_dollar_amt, l_plan_rvu_amt, l_ccs_code, l_ccs_rvu, l_plan_rvu_amt, l_plan_sub_code,
                    l_description, l_sub_desc, l_is_unbillable, l_is_unpayable
                    from HUB_CM_2
                    where hubid is null
                    and agencyid is null
                    and plan_id is null
                    and pathway_id=l_rec.pathway_id 
                    and pathway_step=l_rec.pathway_step
                    and member_type=l_rec.member_type
                    and is_pct=l_rec.is_pct;

                    EXCEPTION WHEN OTHERS THEN
                    BEGIN
                      --if l_rec.member_type in ('A', 'R') then
                        BEGIN
                          select plan_code, plan_dollar_amt, plan_rvu_amt, ccs_code, ccs_rvu, plan_rvu_amt, plan_sub_code,
                          description, sub_desc, IS_UNBILLABLE, IS_UNPAYABLE
                          INTO l_plan_code, l_plan_dollar_amt, l_plan_rvu_amt, l_ccs_code, l_ccs_rvu, l_plan_rvu_amt, l_plan_sub_code,
                          l_description, l_sub_desc, l_is_unbillable, l_is_unpayable
                          from HUB_CM_2
                          where hubid is null
                          and agencyid is null
                          and plan_id is null
                          and pathway_id=l_rec.pathway_id 
                          and pathway_step=l_rec.pathway_step
                          --and member_type in ('A', 'R')
                          and member_type=l_rec.member_type
                          and is_pct=l_rec.is_pct;
                        EXCEPTION
                        WHEN OTHERS THEN
                        BEGIN
                            dbms_output.put_line( 'Querying HUB_CM_2 came up empty!');
                          --ins_pkg_pathway_code_error(p_status_id,p_client_id , p_pathway_id ,p_pathway_step, p_visit_id,p_step_date ,p_error_message ,p_is_pct)  
                          ins_pkg_pathway_code_error(l_rec.status_id, l_rec.client_id, l_rec.pathway_id, l_rec.pathway_step, l_rec.visit_id, 
                          l_rec.step_date, 'HUB_CM_2 unable to find match for'
                            ||' pathway_id='||l_rec.pathway_id||' step='||l_rec.pathway_step||' membertype='||l_rec.member_type||' pct='||l_rec.is_pct
                            ||'  hubid='||l_rec.hubid_4_lookup||'(or null) agency='||l_rec.agencyid_4_lookup||'(or null) planid='||l_rec.plan_id_4_lookup||' (or null)',
                            l_rec.is_pct);
                          l_exception_logged:=true;
                        RAISE_APPLICATION_ERROR( -20043, 'Error in CODE_MASTER queries');
                        END;
                      END;
                      --end if;

                    END;
                  END;
              END;
            END;
          END;
          ------------ IF WE GET HERE AND WE DO NOT HAVE DATA FROM CODE MASTER WE SHOULD EXIT AND CUT OUR LOSSES
          
          ------------ WE HAVE THE CODE MASTER INFORMATION, NOW WE NEED TO GET RVU DETAILS
          
          BEGIN
            dbms_output.put_line( 'Querying HUB_PLANRVU with hubid/agencyid');
           select case when l_rec.member_type='P' then NVL(RVU_PED, RVU_DEFAULT)
                        when l_rec.member_type='R' then NVL(RVU_PREG, RVU_DEFAULT)
                        when l_rec.member_type='A' then NVL(RVU_ADULT, RVU_DEFAULT)
                        when l_rec.member_type='M' then NVL(RVU_MATERNAL, RVU_DEFAULT)
                        when l_rec.member_type='S' then NVL(RVU_SENIOR, RVU_DEFAULT)
                      else null end rvu_value
            INTO l_rvu_value
            FROM HUB_PLANRVU
            WHERE hubid = l_rec.hubid_4_lookup
            and agencyid = l_rec.agencyid_4_lookup
            and plan_id=l_rec.plan_id_4_lookup;
          
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              BEGIN
                  dbms_output.put_line( 'Querying HUB_PLANRVU with hubid/agencyid=null');
                  select case when l_rec.member_type='P' then NVL(RVU_PED, RVU_DEFAULT)
                        when l_rec.member_type='R' then NVL(RVU_PREG, RVU_DEFAULT)
                        when l_rec.member_type='A' then NVL(RVU_ADULT, RVU_DEFAULT)
                        when l_rec.member_type='M' then NVL(RVU_MATERNAL, RVU_DEFAULT)
                        when l_rec.member_type='S' then NVL(RVU_SENIOR, RVU_DEFAULT)
                      else null end rvu_value
                  INTO l_rvu_value
                  FROM HUB_PLANRVU
                  WHERE hubid = l_rec.hubid_4_lookup
                  and agencyid is null
                  and plan_id=l_rec.plan_id_4_lookup;
              EXCEPTION
                WHEN NO_DATA_FOUND THEN
                BEGIN
                  dbms_output.put_line( 'Querying HUB_PLANRVU with hubid/agencyid=null plan_id='||l_rec.plan_id);
                  select case when l_rec.member_type='P' then NVL(RVU_PED, RVU_DEFAULT)
                        when l_rec.member_type='R' then NVL(RVU_PREG, RVU_DEFAULT)
                        when l_rec.member_type='A' then NVL(RVU_ADULT, RVU_DEFAULT)
                        when l_rec.member_type='M' then NVL(RVU_MATERNAL, RVU_DEFAULT)
                        when l_rec.member_type='S' then NVL(RVU_SENIOR, RVU_DEFAULT)
                      else null end rvu_value
                  INTO l_rvu_value
                  FROM HUB_PLANRVU
                  WHERE hubid is null
                  and agencyid is null
                  and plan_id=l_rec.plan_id_4_lookup;
                  
                  EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                  BEGIN
                    dbms_output.put_line( 'Querying HUB_PLANRVU with hubid/agencyid=null abd planid=null');
                    select case when l_rec.member_type='P' then NVL(RVU_PED, RVU_DEFAULT)
                        when l_rec.member_type='R' then NVL(RVU_PREG, RVU_DEFAULT)
                        when l_rec.member_type='A' then NVL(RVU_ADULT, RVU_DEFAULT)
                        when l_rec.member_type='M' then NVL(RVU_MATERNAL, RVU_DEFAULT)
                        when l_rec.member_type='S' then NVL(RVU_SENIOR, RVU_DEFAULT)
                      else null end rvu_value
                    INTO l_rvu_value
                    FROM HUB_PLANRVU
                    WHERE hubid is null
                    and agencyid is null
                    and plan_id is null;
                    EXCEPTION
                    WHEN OTHERS THEN
                    BEGIN
                      dbms_output.put_line( 'Querying HUB_PLANRVU came up empty!');
                      --ins_pkg_pathway_code_error(p_status_id,p_client_id , p_pathway_id ,p_pathway_step, p_visit_id,p_step_date ,p_error_message ,p_is_pct)  
                      ins_pkg_pathway_code_error(l_rec.status_id, l_rec.client_id, l_rec.pathway_id, l_rec.pathway_step, l_rec.visit_id, 
                      l_rec.step_date, 'HUB_PLANRVU unable to find match for hubid='||l_rec.hubid_4_lookup||'(or null) agency='||l_rec.agencyid_4_lookup
                      ||'(or null) planid='||l_rec.plan_id_4_lookup||'(or null)',
                      l_rec.is_pct);
                      l_exception_logged:=true;
                      RAISE_APPLICATION_ERROR( -20043, 'Error in HUB_PLANRVU queries');         
                    END;
                  END;
                END;
              END;
            END;
              
          
          if l_plan_dollar_amt is not null then
            dbms_output.put_line( 'Using Dollar Amount');
            l_code_amt := l_plan_dollar_amt;
          else
            dbms_output.put_line( 'Using RVU Amount');
            l_code_amt :=  l_plan_rvu_amt*l_rvu_value;
          end if;
          
          dbms_output.put_line( 'Code Amount = '||l_code_amt);
          
          -- Special check for Pregnancy pathways that allow multiple payment points
          if l_rec.pathway_id = 13 and l_rec.is_pct='P' then
              select count(*)
              into l_code_check_count
              from CLIENT_CODE_STATUS_2
              where client_id=l_rec.client_id and visit_id=l_rec.visit_id and PLAN_WTRFL_LEVEL <= l_rec.PLAN_WTRFL_LEVEL
              and code_amount > 0
              and ismarked is null
              and pathway_step = l_rec.pathway_step and completed_date = l_rec.completed_date;
          else
          
            select count(*)
            into l_code_check_count
            from CLIENT_CODE_STATUS_2
            where client_id=l_rec.client_id and visit_id=l_rec.visit_id and PLAN_WTRFL_LEVEL <= l_rec.PLAN_WTRFL_LEVEL
            and code_amount > 0
            and ismarked is null;
          END IF;
          
          -- THIS CODE CHECKS TO SEE IF THE CODE HAS ALREADY BEEN SUBMITTED 
          -- THE CHECK SHOULD ALLOW FOR MULTIPLE CODES TO SUBMIT UNDER THE SAME WATERFALL LEVEL
          -- IT SHOULD ALSO ALLOW FOR NULL WATERFALL LEVELS TO SUBMIT MUTIPLE TIMES AND $0 PLAN AMOUNTS SHOULD
          -- NOT DISALLOW THE INSERT EITHER, SO WE ARE RELYING ON ANSI NULL BEHAVIOR HERE
          if l_code_check_count = 0 then
          /**
If in HUB_CM_2 - IS_UNBILLABLE is not null then the CLIENT_CODE_STATUS_2.ISUNBILLABLE = 1, IS_UNBILL_DATE = date of processing, IS_UNBILL_USERNAME='SYSTEM'.
                                         CLIENT_AGENCY_STATUS_2.IS_UNBILLABLE=1,IS_UNBILL_DATE=date of                        processing, IS_UNBILL_USERNAME='SYSTEM'

If in HUB_CM_2 - IS_UNPAYABLE is not null then the CLIENT_CODE_STATUS_2.IS_UNPAYABLE = 1, IS_UNPAY_DATE = date of processing, IS_UNPAY_USERNAME='SYSTEM'.
                                          CLIENT_AGENCY_STATUS_2.IS_UNPAYABLE =1,  then the IS_UNPAY_DATE = date of processing, IS_UNPAY_USERNAME='SYSTEM'.
*/
            if l_is_unbillable is not null then
                l_is_unbillable := '1';
                l_is_unbill_date := SYSDATE;
                l_is_unbill_username := 'SYSTEM';
            else
                l_is_unbillable := null;
                l_is_unbill_date := null;
                l_is_unbill_username := null;
            end if;
            
            if l_is_unpayable is not null then
                l_is_unpayable := '1';
                l_is_unpay_date := SYSDATE;
                l_is_unpay_username := 'SYSTEM';
            else
                l_is_unpayable := null;
                l_is_unpay_date := null;
                l_is_unpay_username := null;
            end if;
            -- override is_unbillable from signer_detail
            if l_signer_isunbillable is not null then
                l_is_unbillable := '1';
                l_is_unbill_date := l_signer_doc_date;
                l_is_unbill_username := l_signer_username;
            end if;
            
            insert into client_code_status_2( client_id, assignto, visit_id, step_date, plan_code, plan_dollar_amt, plan_id,
                                    code_amount, ccs_code, ccs_rvu_amt, plan_rvu_amt, member_type, completed_date, pathway_step, 
                                    is_pct, plan_sub_code,description, sub_desc, pathway_id, hubid, agencyid, insurance_number, riskq_status,
                                    insurance_type, sub_desc_other, PLAN_WTRFL_LEVEL, signer_username, signer_doc_date,
                                    ZIP_CODE, census_tract, ENROLLMENT_DATE, ENROLL_STATUS, STEP_STATUS_ID, is_unbillable, is_unbill_date, is_unbill_username,
                                    is_unpayable, is_unpay_date, is_unpay_username, research_cat_tier1, research_cat_tier2,
                                    HHTA_START_DATE, HHTA_SERVICE_DATE, HHTA_HH_G_CODE, HHTA_SERVICE_LOC, HHTA_APPROVAL_CODE, HHTA_CONSENT, HHTA_CC_NOTE,
                                    HHTA_ID_TIER, HHTA_ID_LOC, HHTA_HAP_CODE, HHTA_EXCEPTION, HHTA_INITIALS , HHTA_ASSESSMENTS , HHTA_QA_ADV_DIR, 
                                    HHTA_QA_CULT_COMP, HHTA_QA_EDIE, HHTA_QA_HAP, HHTA_LORGID, HHTA_READY_TO_BILL, HHTA_DATE_TO_BILL, HHTA_USER_TO_BILL)
            VALUES( l_rec.client_id, l_rec.assignto, l_rec.visit_id, l_rec.step_date, l_plan_code, l_plan_dollar_amt, l_rec.plan_id, 
                    l_code_amt, l_ccs_code, l_ccs_rvu, l_plan_rvu_amt, l_rec.member_type, l_rec.completed_date, l_rec.pathway_step, l_rec.is_pct, 
                    l_plan_sub_code, l_description, l_sub_desc, l_rec.pathway_id, l_rec.hubid, l_rec.agencyid, l_rec.insurance_number, 
                    l_rec.riskq_status, l_rec.insurance_type, l_sub_desc_other, l_rec.PLAN_WTRFL_LEVEL, l_signer_username, l_signer_doc_date,
                    l_rec.ZIP_CODE, l_rec.census_tract, l_rec.ENROLLMENT_DATE, l_rec.ENROLL_STATUS, l_rec.status_id,  l_is_unbillable, 
                    l_is_unbill_date, l_is_unbill_username, l_is_unpayable, l_is_unpay_date, l_is_unpay_username, l_rec.research_cat_tier1, l_rec.research_cat_tier2,
                    l_rec.HHTA_START_DATE, l_rec.HHTA_SERVICE_DATE, l_rec.HHTA_HH_G_CODE, l_rec.HHTA_SERVICE_LOC, l_rec.HHTA_APPROVAL_CODE, 
                    l_rec.HHTA_CONSENT, l_rec.HHTA_CC_NOTE,
                    l_rec.HHTA_ID_TIER, l_rec.HHTA_ID_LOC, l_rec.HHTA_HAP_CODE, l_rec.HHTA_EXCEPTION, l_rec.HHTA_INITIALS , l_rec.HHTA_ASSESSMENTS , 
                    l_rec.HHTA_QA_ADV_DIR, 
                    l_rec.HHTA_QA_CULT_COMP, l_rec.HHTA_QA_EDIE, l_rec.HHTA_QA_HAP, l_rec.HHTA_LORGID, l_rec.HHTA_READY_TO_BILL, l_rec.HHTA_DATE_TO_BILL, 
                    l_rec.HHTA_USER_TO_BILL);
          end if;
          
           dbms_output.put_line( 'Insert into client_code_status_2 done' );
      
      
      if l_step_rec_status = TRUE then
        update client_step_status_2 set is_coded = 'Y' where status_id =l_rec.status_id;
      end if;
      commit;
      l_step_status_processed_cnt := l_step_status_processed_cnt + 1;
  EXCEPTION
  when others then
      if l_exception_logged = false then
        dbms_output.put_line('Exception in client_step_status_2 loop '||SQLERRM);
        ins_pkg_pathway_code_error(l_rec.status_id, l_rec.client_id, l_rec.pathway_id, l_rec.pathway_step, 
        l_rec.visit_id, l_rec.step_date, 'client_step_status_2 loop: '||SQLERRM, l_rec.is_pct);
      end if;
      rollback;
  END;
  end loop; -- end client step status loop
  
  
  --COMMIT;
  
  EXCEPTION
  WHEN OTHERS THEN
    ins_pkg_pathway_code_error(null, null, null, null, null, SYSDATE, SQLERRM, null);
    DBMS_OUTPUT.put_line('Exception '||SQLERRM);
    RAISE_APPLICATION_ERROR(-20000, 'Exception: '||SQLERRM);
   
  END;
  DBMS_OUTPUT.put_line('Processed '||l_step_status_processed_cnt||' client_step_status_2 records');
  
  end client_code_processing;
  
  /*****************************************************************/
procedure client_agency_processing  is
l_count int;
l_code_status_processed_cnt int:=0;
l_agency_pctng_amt HUB_AGENCY_BILLING.agency_pctng_amt%TYPE;
l_agency_dollar_amt HUB_AGENCY_BILLING.agency_dollar_amt%TYPE;
l_exception_logged boolean:=FALSE;
BEGIN
BEGIN
  dbms_output.put_line('Inside client_agency_processing');
 
  -- get the list of steps to iterate over
  for l_rec in (select ccs.*
                from client_code_status_2 ccs 
                where NVL(ccs.is_coded, 'N') ='N'
                and ccs.code_attempted_date is null
                and ccs.ismarked is null)
  LOOP
  BEGIN
      l_exception_logged := FALSE;
      update CLIENT_CODE_STATUS_2 set code_attempted_date = SYSDATE where status_id=l_rec.status_id;
      commit; -- Added on 1/27/2017 to address situation where records just keep getting prcessed over and over
              -- Need to see if this causes probems with the cursor
      
      dbms_output.put_line('');
      --dbms_output.put_line('Status ID:'||l_rec.status_id||' client_id:'||l_rec.client_id||' pathway_id:'||l_rec.pathway_id
      --||' pathway_step:'||l_rec.pathway_step||' hubid:'||l_rec.hubid||' agencyid:'||l_rec.agencyid
      --||' member_type:' ||l_rec.member_type||'Plan Id:'||l_rec.plan_id );
      
          -- CHANGE THIS CODE TO DO THE REQUIRED LOOKUPS 
          -- MATCH ON THE HUBID/AGENCID FIRST AND IF NO MATCH LOOK FOR NULL/NULL
          BEGIN
            dbms_output.put_line( 'Querying HUB_AGENCY_BILLING_2 with hubid/agencyid/plan_id');
            -- FIRST TRY AND MATCH ON THE CLIENT HUBID/AGENCYID
            select agency_pctng_amt, agency_dollar_amt
            INTO l_agency_pctng_amt, l_agency_dollar_amt
            from HUB_AGENCY_BILLING
            where pathway_id=l_rec.pathway_id 
            and pathway_step=l_rec.pathway_step
            and member_type=l_rec.member_type
            and is_pct=l_rec.is_pct
            and hubid = l_rec.hubid
            and agencyid = l_rec.agencyid
            and plan_id=l_rec.plan_id;
          EXCEPTION
          WHEN NO_DATA_FOUND THEN
            BEGIN
              dbms_output.put_line( 'Querying HUB_AGENCY_BILLING with hubid/agencyid=NULL');
              -- SAME QUERY AS ABOVE but now with null HUBID and provided AGENCYID
              select agency_pctng_amt, agency_dollar_amt
              INTO l_agency_pctng_amt, l_agency_dollar_amt
              from HUB_AGENCY_BILLING
              where pathway_id=l_rec.pathway_id 
              and pathway_step=l_rec.pathway_step
              and member_type=l_rec.member_type
              and is_pct=l_rec.is_pct
              and hubid = l_rec.hubid
              and agencyid is null
              and plan_id=l_rec.plan_id;
              
            EXCEPTION
              -- NOW LOOK FOR HUBID is null and AGENCYID is null (defauts)
              WHEN NO_DATA_FOUND THEN
              BEGIN
                dbms_output.put_line( 'Querying HUB_AGENCY_BILLING with hubid=null/agencyid=NULL');
                 select agency_pctng_amt, agency_dollar_amt
                  INTO l_agency_pctng_amt, l_agency_dollar_amt
                  from HUB_AGENCY_BILLING
                  where pathway_id=l_rec.pathway_id 
                  and pathway_step=l_rec.pathway_step
                  and member_type=l_rec.member_type
                  and is_pct=l_rec.is_pct
                  and hubid is null
                  and agencyid is null
                  and plan_id=l_rec.plan_id;
                
                EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                  BEGIN
                    dbms_output.put_line( 'Querying HUB_AGENCY_BILLING with hubid=null/agencyid=NULL/plan_id=NULL');
                    
                    select agency_pctng_amt, agency_dollar_amt
                    INTO l_agency_pctng_amt, l_agency_dollar_amt
                    from HUB_AGENCY_BILLING
                    where pathway_id=l_rec.pathway_id 
                    and pathway_step=l_rec.pathway_step
                    and member_type=l_rec.member_type
                    and is_pct=l_rec.is_pct
                    and hubid is null
                    and agencyid is null
                    and plan_id is null;

                    EXCEPTION WHEN OTHERS THEN
                    BEGIN
                      if l_rec.member_type in ('A', 'R') then
                        BEGIN
                        
                          select agency_pctng_amt, agency_dollar_amt
                          INTO l_agency_pctng_amt, l_agency_dollar_amt
                          from HUB_AGENCY_BILLING
                          where pathway_id=l_rec.pathway_id 
                          and pathway_step=l_rec.pathway_step
                          and member_type  in ('A', 'R')
                          and is_pct=l_rec.is_pct
                          and hubid is null
                          and agencyid is null
                          and plan_id is null;
                    
                        EXCEPTION
                        WHEN OTHERS THEN
                        BEGIN
                            dbms_output.put_line( 'Querying HUB_AGENCY_BILLING came up empty!');
                          ins_pkg_pathway_code_error(l_rec.status_id, l_rec.client_id, l_rec.pathway_id, l_rec.pathway_step, l_rec.visit_id, 
                          l_rec.step_date, 'client_agency_processing: HUB_AGENCY_BILLING unable to find match for'
                            ||' pathway_id='||l_rec.pathway_id||' step='||l_rec.pathway_step||' membertype='||l_rec.member_type||' pct='||l_rec.is_pct
                            ||'  hubid='||l_rec.hubid||'(or null) agency='||l_rec.agencyid||'(or null) planid='||l_rec.plan_id||' (or null)',
                            l_rec.is_pct);
                          l_exception_logged:=true;
                        RAISE_APPLICATION_ERROR( -20043, 'Error in HUB_AGENCY_BILLING queries');
                        END;
                      END;
                      end if;

                    END;
                  END;
              END;
            END;
          END;
          ------------ IF WE GET HERE AND WE DO NOT HAVE DATA FROM CODE MASTER WE SHOULD EXIT AND CUT OUR LOSSES            
  
          if l_agency_dollar_amt is not null then
            l_agency_dollar_amt := l_agency_dollar_amt;
          else
            l_agency_dollar_amt := NVL(l_rec.code_amount,0)*l_agency_pctng_amt;
          end if;

          dbms_output.put_line( 'Agency Dollar Amount = '||l_agency_dollar_amt);
          insert into CLIENT_AGENCY_STATUS_2( client_id, assignto, visit_id, step_date, plan_code, plan_dollar_amt, plan_id,
                                  code_amount, ccs_code, ccs_rvu_amt, plan_rvu_amt, member_type, completed_date, pathway_step, 
                                  is_pct, plan_sub_code, description, sub_desc, pathway_id,
                                  agency_dollar_amt, hubid, agencyid, insurance_number, riskq_status,
                                  insurance_type, signer_doc_date,  signer_username, sub_desc_other,
                                  ZIP_CODE, census_tract, ENROLLMENT_DATE, ENROLL_STATUS, STEP_STATUS_ID, CODE_STATUS_ID,
                                  is_unbillable, is_unbill_date, is_unbill_username, is_unpayable, is_unpay_date, is_unpay_username,
                                  research_cat_tier1, research_cat_tier2,
                                  HHTA_START_DATE, HHTA_SERVICE_DATE, HHTA_HH_G_CODE, HHTA_SERVICE_LOC, HHTA_APPROVAL_CODE, HHTA_CONSENT, HHTA_CC_NOTE,
                                  HHTA_ID_TIER, HHTA_ID_LOC, HHTA_HAP_CODE, HHTA_EXCEPTION, HHTA_INITIALS , HHTA_ASSESSMENTS , HHTA_QA_ADV_DIR, 
                                  HHTA_QA_CULT_COMP, HHTA_QA_EDIE, HHTA_QA_HAP, HHTA_LORGID, HHTA_READY_TO_BILL, HHTA_DATE_TO_BILL, HHTA_USER_TO_BILL)
          VALUES( l_rec.client_id, l_rec.assignto, l_rec.visit_id, l_rec.step_date, l_rec.plan_code, l_rec.plan_dollar_amt, l_rec.plan_id, 
                  l_rec.code_amount, l_rec.ccs_code, l_rec.ccs_rvu_amt, l_rec.plan_rvu_amt, l_rec.member_type, l_rec.completed_date, 
                  l_rec.pathway_step, l_rec.is_pct, l_rec.plan_sub_code, l_rec.description, l_rec.sub_desc, l_rec.pathway_id,
                  l_agency_dollar_amt, l_rec.hubid, l_rec.agencyid, l_rec.insurance_number, l_rec.riskq_status,
                  l_rec.insurance_type, l_rec.signer_doc_date,  l_rec.signer_username, l_rec.sub_desc_other,
                  l_rec.ZIP_CODE, l_rec.census_tract, l_rec.ENROLLMENT_DATE, l_rec.ENROLL_STATUS, l_rec.STEP_STATUS_ID, l_rec.STATUS_ID,
                  l_rec.is_unbillable, l_rec.is_unbill_date, l_rec.is_unbill_username, l_rec.is_unpayable, l_rec.is_unpay_date, l_rec.is_unpay_username,
                  l_rec.research_cat_tier1, l_rec.research_cat_tier2,
                  l_rec.HHTA_START_DATE, l_rec.HHTA_SERVICE_DATE, l_rec.HHTA_HH_G_CODE, l_rec.HHTA_SERVICE_LOC, l_rec.HHTA_APPROVAL_CODE, 
                  l_rec.HHTA_CONSENT, l_rec.HHTA_CC_NOTE, l_rec.HHTA_ID_TIER, l_rec.HHTA_ID_LOC, l_rec.HHTA_HAP_CODE, l_rec.HHTA_EXCEPTION, 
                  l_rec.HHTA_INITIALS , l_rec.HHTA_ASSESSMENTS, l_rec.HHTA_QA_ADV_DIR, l_rec.HHTA_QA_CULT_COMP, 
                  l_rec.HHTA_QA_EDIE, l_rec.HHTA_QA_HAP, l_rec.HHTA_LORGID, l_rec.HHTA_READY_TO_BILL, l_rec.HHTA_DATE_TO_BILL, 
                    l_rec.HHTA_USER_TO_BILL
                  );
          
           dbms_output.put_line( 'Insert into CLIENT_AGENCY_STATUS_2 done' );
      
      
        -- set is_coded
        update CLIENT_CODE_STATUS_2 set is_coded = 'Y' where status_id =l_rec.status_id;
      
      commit;
      l_code_status_processed_cnt := l_code_status_processed_cnt + 1;
  EXCEPTION
  when others then
      if l_exception_logged = false then
        dbms_output.put_line('Exception in client_step_status_2 loop '||SQLERRM);
        ins_pkg_pathway_code_error(l_rec.status_id, l_rec.client_id, l_rec.pathway_id, l_rec.pathway_step, 
        l_rec.visit_id, l_rec.step_date, 'client_agency_processing: '||SQLERRM, l_rec.is_pct);
      end if;
      rollback;
  END;
  end loop; -- end client code status loop
  
  
  --COMMIT;
  
  EXCEPTION
  WHEN OTHERS THEN
    ins_pkg_pathway_code_error(null, null, null, null, null, SYSDATE, SQLERRM, null);
    DBMS_OUTPUT.put_line('Exception '||SQLERRM);
    RAISE_APPLICATION_ERROR(-20000, 'Exception: '||SQLERRM);
   
  END;
  DBMS_OUTPUT.put_line('Processed '||l_code_status_processed_cnt||' client_code_status_2 records');
  
end client_agency_processing;
  
  
  -- A step may have more than one condition to check, if any are true the step
  -- has passed
  
/***************************************************************************/
  procedure reset_all_pathway_steps is
  begin
  
    delete from client_step_status_2;
    delete from pkg_pathway_step_errors;
    update pathway_education set steps_completed = null;
    update clients_pathways set steps_completed=null, completed_step=null,completed_status=null,completed_date = null;
    
    commit;
end reset_all_pathway_steps;

/***************************************************************************/
  procedure calculate_in_queue is
  l_step number;
  l_sql varchar2(100);
  l_status boolean;
  begin
  BEGIN
  
    dbms_output.put_line('Starting...');
    for l_rec in (select * from queue_pathway_steps where attempted_date is null
                  order by insert_date)
    loop  
    BEGIN
      -- maintain_most_recent_flag( l_rec.pathway_table_name, l_rec.id );
      l_status := true; -- inititalize status to success
      l_step := pathway_step_processing(L_REC.pathway_table_name, l_rec.id, l_status);
      dbms_output.put_line('Returned step '||l_step);
      if l_step is not null and L_REC.pathway_table_name like 'PATHWAY%' then
        l_sql := 'update '||L_REC.pathway_table_name||' set STEPS_COMPLETED = '||l_step
                ||' WHERE ID = '''||l_rec.id||'''';
         dbms_output.put_line(l_sql);
        execute IMMEDIATE l_sql;
      end if;
      
      -- If no errors detected then mark attempted and processed
      if l_status = true then
        update queue_pathway_steps set attempted_date=sysdate, processed_date=sysdate
        where queue_pathway_steps_id = l_rec.queue_pathway_steps_id;
      else
      -- If we hit an error then just mark attempted
         update queue_pathway_steps set attempted_date=sysdate
        where queue_pathway_steps_id = l_rec.queue_pathway_steps_id;
      end if;
      
      commit;
    EXCEPTION
    WHEN OTHERS THEN
      ins_pkg_pathway_step_error( L_REC.pathway_table_name, -99, SQLERRM, null, null, null);
      rollback;
    END;
    end loop;
    -- Only delete processed records older than 30 days
    delete from queue_pathway_steps where processed_date < trunc(SYSDATE-30);
    commit;
  EXCEPTION WHEN OTHERS
  THEN
    ins_pkg_pathway_step_error( 'ALL', -99, SQLERRM, null, null, null);
  END;
    
  end calculate_in_queue;

/************************************************************************/
procedure assign_null_research_tiers is
begin
  
    update clients_pathways
    set research_cat_tier1=get_research_cat_tier1(clientid, pathway_id, 'CODE_NUMBER', code_number, 'P'),
    research_cat_tier2=get_research_cat_tier2(clientid, pathway_id, 'CODE_NUMBER', code_number, 'P')
    where pathway_id=14
    and (research_cat_tier1 is null or research_cat_tier2 is null);
    COMMIT;

    update clients_pathways
    set research_cat_tier1=get_research_cat_tier1(clientid, pathway_id, 'REFERRAL_TYPE', referral_type, 'P'),
    research_cat_tier2=get_research_cat_tier2(clientid, pathway_id, 'REFERRAL_TYPE', referral_type, 'P')
    where pathway_id=11
    and (research_cat_tier1 is null or research_cat_tier2 is null);
    COMMIT;

    update clients_pathways
    set research_cat_tier1=get_research_cat_tier1(clientid, pathway_id, 'MODULE_TYPE', module_type, 'P'),
    research_cat_tier2=get_research_cat_tier2(clientid, pathway_id, 'MODULE_TYPE', module_type, 'P')
    where pathway_id=17 
    and (research_cat_tier1 is null or research_cat_tier2 is null);

    COMMIT;
end assign_null_research_tiers;
  
  /***************************************************************************
  * Procedure re-processes queue records from the previous day so EOD info
  * can be used to evaluate.
  ***************************************************************************/
  procedure process_queue_eod is
  l_step number;
  l_sql varchar2(100);
  l_status boolean;
  begin
  BEGIN
  
    dbms_output.put_line('Starting...');
    for l_rec in (select * from queue_pathway_steps 
                  where insert_date >= TRUNC(SYSDATE-1) and insert_date < TRUNC(SYSDATE)
                  and attempted_date is not null
                  order by insert_date)
    loop  
    BEGIN
      l_status := true; -- inititalize status to success
      l_step := pathway_step_processing(L_REC.pathway_table_name, l_rec.id, l_status);
      dbms_output.put_line('Returned step '||l_step);
      if l_step is not null and L_REC.pathway_table_name like 'PATHWAY%' then
        l_sql := 'update '||L_REC.pathway_table_name||' set STEPS_COMPLETED = '||l_step
                ||' WHERE ID = '''||l_rec.id||'''';
         dbms_output.put_line(l_sql);
        execute IMMEDIATE l_sql;
      end if;
      
      -- If no errors detected then mark attempted and processed
      if l_status = true then
        update queue_pathway_steps set attempted_date=sysdate, processed_date=sysdate
        where queue_pathway_steps_id = l_rec.queue_pathway_steps_id;
      else
      -- If we hit an error then just mark attempted
         update queue_pathway_steps set attempted_date=sysdate
        where queue_pathway_steps_id = l_rec.queue_pathway_steps_id;
      end if;
      
      commit;
      
    EXCEPTION
    WHEN OTHERS THEN
      ins_pkg_pathway_step_error( L_REC.pathway_table_name, -99, SQLERRM, null, null, null);
      rollback;
    END;
    end loop;
    -- Only delete processed records older than 30 days
    delete from queue_pathway_steps where processed_date < trunc(SYSDATE-30);
    commit;
    
    assign_null_research_tiers;
    
  EXCEPTION WHEN OTHERS
  THEN
    ins_pkg_pathway_step_error( 'ALL', -99, SQLERRM, null, null, null);
  END;
    
  end process_queue_eod;
  
  /***************************************************************************/
  procedure job_processing is
  begin
    process_client_enrollment_q;
    calculate_in_queue;
    client_code_processing;
    client_agency_processing;
  end job_processing;
  
   
/***************************************************************************/
procedure generate_executions( p_type VARCHAR2 default null, p_name VARCHAR2 default null, p_client_id varchar2 default null) AS
l_sql varchar2(1000);
l_id_query varchar2(4000);
l_id varchar2(30);
l_record_date varchar2(20);
TYPE cur_typ IS REF CURSOR;
c cur_typ;

  begin
    delete from steps_commands;
    commit;
    -- Register any new tables into the ALL_PW_CL_TL table
    insert into ALL_PW_CL_TL(table_name, table_type, id_column_name)
    select distinct upper(pathway_table), 
    case when IS_PCT='C' then 'CHECKLIST' when IS_PCT='P' then 'PATHWAY' when IS_PCT='T' then 'TOOL' end table_type, 'ID' 
    from hub_sm_2 where is_pct in ('C', 'P','T')
    and upper(pathway_table) in (select table_name from user_tables)
    MINUS
    select table_name, table_type, 'ID' from ALL_PW_CL_TL ;
    commit;
    
    for l_rec in (select table_name, table_type from ALL_PW_CL_TL where table_type = NVL(p_type, table_type) 
                  and table_name = NVL(p_name, table_name) ) LOOP
      l_id_query := null;
      if l_rec.table_type = 'PATHWAY' then
        l_id_query := 'select p.id, p.recoredupdateddate from '|| l_rec.table_name||' p'
                  ||' join clients_pathways cp on cp.client_pathway_visitid = p.visit_id'
                  ||' join clients c on c.client_id = cp.clientid'
                  ||' where c.ismarked is NOT null and cp.ismarked is null'
                  ||' and c.hubid = ''31'' '
                  ||' and cp.recordcreateddate>=''2018-08-09''  '
                  --||' and c.hubid in (''5'', ''6'', ''7'', ''9'') '
                  --||' and p.recordcreateddate>=''2015-01''  '
                  ;
        if p_client_id is not null then
          l_id_query := l_id_query ||' and c.client_id = '''||p_client_id||'''';
        end if;
                  
         l_id_query :=   l_id_query ||' order by cp.recordcreateddate';               
       end if;
       if l_rec.table_type = 'CHECKLIST' then
        l_id_query := 'select p.id,p.recoredupdateddate from '|| l_rec.table_name||' p'
                  ||' join client_checklists cp on cp.client_checklist_visitid = p.visit_id'
                  ||' join clients c on c.client_id = cp.client_id'
                  ||' where c.ismarked is NOT null and cp.ismarked is null'
                  ||' and c.hubid = ''31'' '
                  ||' and cp.recordcreateddate>=''2018-08-09''  '
                  --||' and c.hubid in (''5'', ''6'', ''7'', ''9'') '
                  --||' and p.recordcreateddate>=''2015-01''  '
                  ;
        if p_client_id is not null then
          l_id_query := l_id_query ||' and c.client_id = '''||p_client_id||'''';
        end if;
                  
         l_id_query :=   l_id_query ||' order by cp.recordcreateddate';              
       end if;
  
      if l_rec.table_type = 'TOOL' then
        l_id_query := 'select p.id,p.recoredupdateddate from '|| l_rec.table_name||' p'
                  ||' join clients_tools cp on cp.client_tool_visitid = p.visit_id'
                  ||' join clients c on c.client_id = cp.clientid'
                  ||' where c.ismarked is NOT null and cp.ismarked is null'
                  ||' and c.hubid = ''31'' '
                  ||' and cp.recordcreateddate>=''2018-08-09''  '
                  --||' and c.hubid in (''5'', ''6'', ''7'', ''9'') '
                  --||' and p.recordcreateddate>=''2015-01'' '
                  ;
        if p_client_id is not null then
          l_id_query := l_id_query ||' and c.client_id = '''||p_client_id||'''';
        end if;
                  
         l_id_query :=   l_id_query ||' order by cp.recordcreateddate';                
       end if;
       
       if l_rec.table_type = 'CLIENTS' then
        l_id_query := 'select p.id, p.recoredupdateddate from '|| l_rec.table_name||' p'
                  ||' where p.ismarked is NOT null'
                  ||' and p.hubid = ''31'' '
                  ||' and p.recordcreateddate>=''2018-08-09'' '
                  ;
        if p_client_id is not null then
          l_id_query := l_id_query ||' and c.client_id = '''||p_client_id||'''';
        end if;
                  
         l_id_query :=   l_id_query ||' order by p.recordcreateddate';                
       end if;
       
       if l_id_query is not null then
       BEGIN
       open c for l_id_query; 
        LOOP
          FETCH c into l_id, l_record_date;
          EXIT WHEN c%NOTFOUND;
          --l_sql := 'exec pkg_pathway_steps_2.test_pathway_step_processing('''||l_rec.table_name||''', '''||l_id||''');';
          l_sql := 'exec pkg_pathway_steps_2.test_pathway_step_processing('''||l_rec.table_name||''', '''||l_id||''');';
          INSERT INTO steps_commands(RUN_COMMAND, command_template, bind_table, bind_table_id, record_date) VALUES(l_sql,
          'BEGIN pkg_pathway_steps_2.test_pathway_step_processing( :bind_table, :bind_table_id); END;', l_rec.table_name, l_id, l_record_date );
          --dbms_output.put_line( l_sql );
          --execute immediate l_insert_sql;
          commit;
          
        end LOOP; -- for an indivdual table
        close c;
        --deallocate c;
      EXCEPTION
      when others then
          dbms_output.put_line( 'Error on table '||l_rec.table_name||' '||SQLERRM);
      end;
     
    END IF;
    end loop;
    
end generate_executions;

/***************************************************************************/
procedure generate_executions_from_errs AS
l_sql varchar2(1000);
l_id_query varchar2(4000);
l_id varchar2(30);

  begin
    delete from steps_commands;
  
    
    for l_rec in (select pathway_table_name, pathway_table_id from PKG_PATHWAY_STEP_ERRORS ) 
    LOOP
      BEGIN
          INSERT INTO steps_commands(RUN_COMMAND, command_template, bind_table, bind_table_id) VALUES(l_sql,
          'BEGIN pkg_pathway_steps_2.test_pathway_step_processing( :bind_table, :bind_table_id); END;', l_rec.pathway_table_name, l_rec.pathway_table_id );    
      EXCEPTION
      when others then
          rollback;
          dbms_output.put_line( 'Error on table '||l_rec.pathway_table_name||' id='||l_rec.pathway_table_id||' '||SQLERRM);
      end;
     
    end loop;
    commit;
    
end generate_executions_from_errs;

/***************************************************************************/
procedure run_steps_executions AS
l_call varchar2(4000);
  begin
    
    for l_rec in (select id, command_template, bind_table, bind_table_id from steps_commands where processed_date is null order by record_date) LOOP
    BEGIN  
      execute immediate l_rec.command_template USING l_rec.bind_table, l_rec.bind_table_id;
      update steps_commands set processed_date = SYSDATE where id = l_rec.id;
      commit;
    EXCEPTION
    when others then
      ins_pkg_pathway_step_error( 'steps_commands', -99, 'Error processing id='||l_rec.id||' '||SQLERRM, null, null, null);
      rollback;
    end;
    end loop; 

end run_steps_executions;

/***************************************************************************/
procedure reprocess_code_status_id ( p_code_status_id varchar2 ) AS
 l_step_status_id client_step_status_2.status_id%TYPE;
 l_client_id client_step_status_2.client_id%TYPE;
 l_pathway_id client_step_status_2.pathway_id%TYPE;
 l_pathway_step client_step_status_2.pathway_step%TYPE;
 l_visit_id client_step_status_2.visit_id%TYPE;
 l_step_date client_step_status_2.step_date%TYPE;
 l_is_pct client_step_status_2.is_pct%TYPE;
begin
  update CLIENT_STEP_STATUS_2 SET IS_CODED='N',CODE_ATTEMPTED_DATE=NULL WHERE ISMARKED IS NULL and status_id=(
    select step_status_id from client_code_status_2 where status_id = p_code_status_id );

  UPDATE CLIENT_CODE_STATUS_2
    SET IS_CODED='N',CODE_ATTEMPTED_DATE=NULL, ISMARKED='1', ISMARKED_DATE=SYSDATE, INVOICE_NUMBER=NULL, INVOICE_DATE=NULL, FULL_PYMT_DATE=NULL
    WHERE STATUS_ID=p_code_status_id and ismarked is null;
    
  
  UPDATE CLIENT_AGENCY_STATUS_2 SET ISMARKED='1', ISMARKED_DATE=SYSDATE WHERE CODE_STATUS_ID=p_code_status_id;
  
  insert INTO sp_log(log_date, package_name, procedure_name, message )
  values(SYSDATE, 'PKG_PATHWAY_STEPS_2', 'reprocess_code_status_id', 'CODE_STATUS_ID '||p_code_status_id||' reprocessed');
  
  commit;
  
  EXCEPTION
  when others then
      rollback;
      select status_id, client_id, pathway_id, pathway_step, visit_id, step_date, is_pct
      into l_step_status_id, l_client_id, l_pathway_id, l_pathway_step, l_visit_id, l_step_date, l_is_pct
      from client_step_status_2 where status_id = (select step_status_id from client_code_status_2 where status_id=p_code_status_id);
      ins_pkg_pathway_code_error( l_step_status_id, l_client_id, l_pathway_id, l_pathway_step, l_visit_id, l_step_date, SQLERRM, l_is_pct);
  end;




end;