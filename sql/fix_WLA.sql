--skewed projections
--alter tuning rule sys_r3 set min_avg_row_count=100000 ;
--alter tuning rule sys_r3 set skew_pct=20 ;

--sys_r9 reports on superusers with empty passwords.
alter tuning rule sys_r9 disable ;
alter tuning rule sys_r3 disable ;  -- doesn't really work, replaced by user_r13


--cpu usage
alter tuning rule sys_r16 set usage_threshold_pct=80 ;
alter tuning rule sys_r16 set duration_min=5 ;

--unused projections
alter tuning rule sys_r17 set past_days=14;
alter tuning rule sys_r17 set table_used_at_least=5 ;
alter tuning rule sys_r17 set min_used_bytes=10000 ;

--memory usage
alter tuning rule sys_r20 set usage_threshold_pct=90;
alter tuning rule sys_r20 set duration_min=5 ;


--current version of r4 is commented out. This recreates it, with the caveat of a "large_cluster" check.
--Change the large_cluster parameter (in the next line) as needed.
CREATE TUNING RULE user_sys_r4 (SEG_SMALL_TABLE, REP_SMALL_TABLE, max_row_count = 10000, large_cluster=40) as
select NULL as time
     , 'small table ' || t.table_schema || '.' || t.table_name || ' is not replicated' as observation_description
     , t.table_id as table_id
     , NULL as transaction_id
     , NULL as statement_id
     , t.table_schema || '.' || t.table_name as tuning_parameter
     , 'create replicated projection for table ' || t.table_schema || '.' || t.table_name as tuning_description
     , NULL as tuning_command
     , 'LOW' as tuning_cost
 from ( select distinct t.table_schema, t.table_name, t.table_id 
          from v_catalog.tables t
             , v_catalog.projections p
             , v_internal.vs_projection_columns pc 
         where t.table_id = p.anchor_table_id 
           and p.projection_id = pc.proj 
           and row_count > 0 
           and row_count < (select current_value from v_internal.vs_tuning_rule_parameters 
                    where tuning_rule = 'user_sys_r4' and parameter = 'max_row_count') 
           and p.anchor_table_id not in (select anchortable 
                                           from v_internal.vs_projections 
                                          where seginfo = 0) 
           and (select count(*) from v_internal.vs_nodes) < (select current_Value from v_internal.vs_tuning_rule_parameters
                                                              where tuning_rule = 'user_sys_r4' and parameter = 'large_cluster')
      ) t ;

--current version of r12 does not work. This fixes it.
CREATE TUNING RULE user_sys_r12 (LGE_LAGGING, SET_CONFIG_PARAM) as
select event_posted_timestamp as time
     , event_problem_description as observation_description
     , NULL as table_id
     , NULL as transaction_id
     , NULL as statement_id
     , 'MoveOutInterval' as tuning_parameter
     , 'decrease ''MoveOutInterval'' configuration parameter setting' as tuning_description
     , 'select set_config_parameter(''MoveOutInterval'', ' || 
(select default_value from v_internal.vs_configuration_parameters where parameter_name = 'MoveOutInterval') || ')' as tuning_command
     , 'LOW' as tuning_cost
 from v_monitor.active_events e
 where event_code = 15
   and exists (select 'x'
                 from v_internal.vs_configuration_parameters
                where parameter_name = 'MoveOutInterval'
                  and current_value::int > default_value::int) ;


--simplified and expanded version of the current r15.
CREATE TUNING RULE user_sys_r15 (ROS_PUSHBACK, ENABLE_SERVICE) as
select null AS time
      , service_name || ' is disabled.' as observation_description
      , NULL as table_id
      , NULL as transaction_id
      , NULL as statement_id
      , 'enable_service()' as tuning_parameter
      , 'ensure ''' || service_name || ''' service is enabled' as tuning_description
      , CASE when service_type = 'System' then
'select enable_service(''System'', ''' || service_name || ''')' 
        when service_type = 'Tuple Mover' then 
'select enable_service(''TM'', ''' || service_name || ''')'  END
as tuning_command
      , 'LOW' as tuning_cost
  from v_monitor.system_services svc
 where (not svc.is_enabled) 
   and service_type in ('Tuple Mover') 
   and svc.service_name != 'PartitionTables' ;

