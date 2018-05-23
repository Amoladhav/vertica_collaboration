-- checks disk free percent. default 40% recommendation
CREATE tuning rule user_r1 (DISK_SPACE, SYSTEM, free_disk_percent=40 ) AS
SELECT CURRENT_TIME AS time
     , 'Free disk space is below recommended levels - ' || ROUND((disk_space_free_mb / disk_space_total_mb), 2)*100::CHAR(10) || '% free.' AS observation_description 
     , NULL AS table_id
     , NULL AS transaction_id
     , NULL AS statement_id
     , (SELECT current_value || '%' 
          FROM v_internal.vs_tuning_rule_parameters
         WHERE tuning_rule = 'user_r1' 
           AND parameter = 'free_disk_percent') AS tuning_parameter
     , 'Free up some disk space!' AS tuning_description
     , 'SELECT * FROM v_monitor.host_resources' AS tuning_command
     , 'HIGH' AS tuning_cost 
FROM v_monitor.host_resources 
WHERE disk_space_total_mb > 0
  AND disk_space_free_mb / disk_space_total_mb < (SELECT current_value/100::NUMERIC 
                                                    FROM v_internal.vs_tuning_rule_parameters
                                                   WHERE tuning_rule = 'user_r1'
                                                     AND parameter = 'free_disk_percent') ;

-- checks license size threshold. Default 80% utilization.
CREATE tuning rule user_r2 (LICENSE_SIZE, LICENSE, license_usage_percent=80 ) AS 
SELECT CURRENT_TIME AS time
     , 'License usage size exceeds warning threshold. License usage at ' || ROUND(usage_percent, 2)*100::CHAR(5) || '% utilization.' AS observation_description
     , NULL AS table_id
     , NULL AS transaction_id
     , NULL AS statement_id
     , (SELECT current_value 
          FROM v_internal.vs_tuning_rule_parameters
         WHERE tuning_rule = 'user_r2' 
           AND parameter = 'license_usage_percent') AS tuning_parameter
     , 'Contact HP/Vertica support' AS tuning_description
     , 'SELECT get_compliance_status();' AS tuning_command
     , 'HIGH' AS tuning_cost
  FROM v_catalog.license_audits
 WHERE audit_end_timestamp = (SELECT MAX(audit_end_timestamp) 
                                FROM v_catalog.license_audits)
   AND ROUND(usage_percent,2)*100 >= (SELECT current_value::NUMERIC 
                                        FROM v_internal.vs_tuning_rule_parameters
                                       WHERE tuning_rule = 'user_r2'
                                         AND parameter = 'license_usage_percent') ;

-- checks for large tables (> 5M rows) that are not partitioned, but that do have a date or time column in them.
CREATE tuning rule user_r3 (PARTITION_TABLE, ALTER_TABLE, table_size_threshold=5000000 ) AS
SELECT CURRENT_TIME AS time
     , 'Table ' || projection_schema || '.' || anchor_table_name || ' might be a good candidate for partitioning. It is large, unpartitioned, and has date and/or time values present which could be good candidate for partitioning'  AS observation_description
     , anchor_table_id AS table_id
     , anchor_table_name AS table_name
     , s.schema_id AS table_schema_id
     , projection_schema AS table_schema
     , NULL AS transaction_id
     , NULL AS statement_id
     , (SELECT current_value 
          FROM v_internal.vs_tuning_rule_parameters
         WHERE tuning_rule = 'user_r3' 
           AND parameter = 'table_size_threshold') AS tuning_parameter
     , 'Table might be a good candidate for partitioning' AS tuning_description
     , 'alter table ' || projection_schema || '.' || anchor_table_name || ' partition by *column_name* reorganize;' AS tuning_command
     , 'MEDIUM' AS tuning_cost
  FROM v_monitor.projection_storage ps 
  JOIN v_catalog.schemata s ON ps.projection_schema = s.schema_name
 WHERE row_count >= (SELECT current_value::NUMERIC FROM v_internal.vs_tuning_rule_parameters
                      WHERE tuning_rule = 'user_r3' 
                        AND parameter = 'table_size_threshold')
   AND NOT EXISTS(SELECT 'x' FROM v_monitor.partitions p WHERE p.projection_name = ps.projection_name)
   AND ((EXISTS (SELECT 'x' FROM v_catalog.projection_columns pc WHERE pc.projection_name = ps.projection_name AND data_type LIKE 'time%') )
    OR  (EXISTS (SELECT 'x' FROM v_catalog.projection_columns pc WHERE pc.projection_name = ps.projection_name AND data_type = 'date') )) ;

--Checks for too many delete vectors. shouldn't be more than 20% of the total rows on that table.
CREATE tuning rule user_r4 (DELETE_VECTORS, PURGE, dv_threshold=20 ) AS
SELECT CURRENT_TIME AS time
     , 'Delete vectors account for ' || ROUND(deleted_row_count/row_count*100, 2)::CHAR(4) || '% of rows for projection ' || ps.projection_name  AS observation_description
     , ps.anchor_table_id AS table_id
     , ps.anchor_table_name AS table_name
     , s.schema_id AS table_schema_id
     , ps.projection_schema AS table_schema
     , NULL AS transaction_id
     , NULL AS statement_id
     , (SELECT current_value || '%' 
          FROM v_internal.vs_tuning_rule_parameters
         WHERE tuning_rule = 'user_r4' 
           AND parameter = 'dv_threshold') AS tuning_parameter
     , 'Run purge operations on tables with delete vectors.' AS tuning_description
     , 'SELECT purge_projection(''' || ps.projection_name || ''');' AS tuning_command
     , 'LOW' AS tuning_cost
  FROM v_monitor.delete_vectors dv
  JOIN v_monitor.projection_storage ps ON dv.projection_name = ps.projection_name
  JOIN v_catalog.schemata s ON s.schema_name = dv.schema_name
 WHERE ps.row_count > 0
   AND deleted_row_count / row_count > (SELECT current_value/100::NUMERIC 
                                          FROM v_internal.vs_tuning_rule_parameters
                                         WHERE tuning_rule = 'user_r4'
                                           AND parameter = 'dv_threshold') ;

--Checks for tables which are partitioned on something other than a date column - can cause mergeout issues.
--last line fails in version 6.
CREATE tuning rule user_r5 (PARTITION_KEYS, ALTER_TABLE) AS
SELECT current_time AS time
     , 'Table ' || t.table_schema || '.' || t.table_name || ' is currently partitioned on column ' || c.column_name || ', which is of type ' || c.data_type  AS observation_description
     , t.table_id AS table_id
     , t.table_name AS table_name
     , t.table_schema_id AS table_schema_id
     , t.table_schema AS table_schema
     , NULL AS transaction_id
     , NULL AS statement_id
     , NULL AS tuning_parameter
     , 'Table is partitioned on something other than a date or time field. This could affect the efficiency of mergeouts.' AS tuning_description
     , 'alter table ' || t.table_schema || '.' || t.table_name || ' partition by *column_name* reorganize;' AS tuning_command
     , 'MEDIUM' AS tuning_cost
  FROM v_catalog.tables t
     , v_catalog.columns c
     --, v_monitor.partitions p
     --, v_catalog.projections j
 WHERE length(t.partition_expression) > 0
   AND t.table_id = c.table_id
   AND instr(lower(t.partition_expression), lower(c.column_name)) > 0
   AND c.data_type not ilike 'time%'
   AND c.data_type not ilike 'date%' ;
   --AND j.anchor_table_id = t.table_id
   --AND p.projection_id = j.projection_id
   --AND count(distinct p.partition_key) * 3 > (SELECT current_value 
                                                  --FROM v_monitor.configuration_parameters WHERE parameter_name = 'ActivePartitionCount') ;
   --AND (SELECT current_value FROM v_monitor.configuration_parameters WHERE parameter_name = 'ActivePartitionCount') <= (Select count(distinct p2.partition_key) * 3 FROM partitions p2 WHERE p2.projection_id = p.projection_id) ;


--checks for license expiration date coming close
--only works in v7. 
CREATE tuning rule user_r6 (LICENSE_DATE, LICENSE, days_away=60) AS
SELECT current_time AS time
     , 'Vertica license expires on ' || l.end_date || '.'  AS observation_description
     , NULL AS table_id
     , NULL AS table_name
     , NULL AS table_schema_id
     , NULL AS table_schema
     , NULL AS transaction_id
     , NULL AS statement_id
     , NULL AS tuning_parameter
     , 'Contact Vertica support.' AS tuning_description
     , 'SELECT get_compliance_status() ;' AS tuning_command
     , 'MEDIUM' AS tuning_cost
FROM v_catalog.licenses l
WHERE end_date != 'Perpetual'
  AND end_date::date - (SELECT CURRENT_VALUE 
                          FROM v_internal.vs_tuning_rule_parameters 
                         WHERE tuning_rule = 'user_r6' 
                           AND parameter = 'days_away')::INTERVAL DAY >= SYSDATE ;


--checks for differences between nodes for host_resource values.
CREATE TUNING RULE user_r7 (OS_CONFIG, SYSTEM) AS
SELECT CURRENT_TIME AS time
     , 'System configuration parameter not consistent across nodes' AS observation_description
     , NULL AS table_id
     , NULL AS transaction_id
     , NULL AS statement_id
     , 'v_monitor.host_resources' AS tuning_parameter
     , DECODE(attrib 
        ,'open_files_limit', 'ulimit -n reporting inconsistent results across the cluster'
        ,'threads_limit', 'ulimit -u reporting inconsisten results across the cluster'
        ,'processor_count', 'inconsistent processor count on nodes within the cluster' 
        ,'processor_core_count', 'inconsistent processor core count on nodes within the cluster'
        ,'processor_description', 'inconsistent processor speed and/or manufacturer on nodes within the cluster'
        ,'total_memory_bytes', 'total memory sizes vary across nodes in the cluster'
        ,'total_swap_memory_bytes', 'swap space not consistently defined across all nodes in the cluster'
        ,'disk_space_total_mb', 'disk space allocation is not consistent on all nodes in the cluster' ) AS tuning_description
     , DECODE(attrib 
        ,'open_files_limit', 'define consistent NOFILES in /etc/security/limits.conf across all nodes in the cluster'
        ,'threads_limit', 'define consisten NPROC in /etc/security/limits.conf across all nodes in the cluster'
        ,'processor_count', 'For optimum performance, all node hardware should be consistent'
        ,'processor_core_count', 'for optimum performance, all node hardware should be consistent' 
        ,'processor_description', 'for optimum performance, all node hardware should be consistent'
        ,'total_memory_bytes', 'memory allocations should be consistently defined across all nodes in the cluster'
        ,'total_swap_memory_bytes', 'swap space should be 2GB minimum on all nodes and consistently defined on all nodes in the cluster'
        ,'disk_space_total_mb', 'for optimum performance, all nodes should have equivalent disk storage capacity' ) AS tuning_command
     , 'HIGH' AS tuning_cost
  FROM (SELECT 'open_files_limit'::VARCHAR(30) AS attrib
             , COUNT(distinct open_files_limit) AS cnt
          FROM v_monitor.host_resources 
         UNION
        SELECT 'threads_limit'::VARCHAR(30) AS attrib
             , COUNT(distinct threads_limit) AS cnt
          FROM v_monitor.host_resources 
         UNION
        SELECT 'processor_count'::VARCHAR(30) AS attrib
             , COUNT(distinct processor_count) AS cnt
          FROM v_monitor.host_resources 
         UNION
        SELECT 'processor_core_count'::VARCHAR(30) AS attrib
             ,  COUNT(distinct processor_core_count) AS cnt
          FROM v_monitor.host_resources 
         UNION
        SELECT 'processor_description'::VARCHAR(30) AS attrib
             ,  COUNT(distinct processor_description) AS cnt
          FROM v_monitor.host_resources 
         UNION
        SELECT 'total_memory_bytes'::VARCHAR(30) AS attrib
             ,  COUNT(distinct total_memory_bytes) AS cnt
          FROM v_monitor.host_resources 
         UNION
        SELECT 'total_swap_memory_bytes'::VARCHAR(30) AS attrib
             ,  COUNT(distinct total_swap_memory_bytes) AS cnt
          FROM v_monitor.host_resources 
         UNION
        SELECT 'disk_space_total_mb'::VARCHAR(30) AS attrib
             ,  COUNT(distinct disk_space_total_mb) AS cnt
          FROM v_monitor.host_resources) AS hr 
 WHERE cnt > 1;

--Complicated query. Checks for node dependencie skew. Warns when there is invalid projection node dependencies.
CREATE TUNING RULE user_r8 (DEPENDENCIES, KSAFETY) AS
SELECT NULL AS time
     , 'Node/projection dependencies are not KSAFE' AS observation_description
     , NULL AS table_id
     , NULL AS transaction_id
     , NULL AS statement_id
     , 'SELECT get_node_dependencies() ;' AS tuning_parameter
     , 'Projection definitions are not evenly distributed throughout the cluster' AS tuning_description
     , 'SELECT rebalance_cluster() ;'  AS tuning_command
     , 'HIGH' AS tuning_cost
  FROM (SELECT DISTINCT dependency_id
             , COUNT(node_oid) OVER (PARTITION BY dependency_id) AS cnt
             , AVG(ref_count) OVER (PARTITION BY dependency_id) AS dependency_count 
          FROM v_internal.vs_node_dependencies ) AS nd
 WHERE cnt < (SELECT count(*) FROM v_internal.vs_nodes) 
 GROUP BY dependency_count 
HAVING COUNT(*) != (SELECT COUNT(*) FROM v_internal.vs_nodes) LIMIT 1;

--Recurring login failures
--A recurring login failure is a user who has failed to sign in at roughly the same time across multiple days
--This could likely be the result of an automated script running in Cron which is failing.
CREATE TUNING RULE user_r9 (LOGIN_FAILURES, RECURRING_FAILURES, days_to_check=14, attempts=3) AS
SELECT NULL AS time
      ,'User ''' || user_name || ''' had ' || num_failures || ' failed signon attempts at ' ||
        hour_minute || ' between ' || begin_t || ' and ' || end_t || '.' AS observation_description
      , NULL AS table_id
      , NULL AS transaction_id
      , NULL AS statement_id
      , 'SELECT * FROM v_monitor.login_failures WHERE user_name = ''' || user_name || ''';' AS tuning_parameter
      , 'Recurring login failures are a likely indication of a problem in an automated script or process.' AS tuning_description
      , 'Investigate the source of the login failures and correct as necessary.' AS tuning_command
      , 'MEDIUM' AS tuning_cost
   FROM (SELECT user_name
              , TRIM(TO_CHAR(HOUR(login_Timestamp), '00')) || ':' || TRIM(TO_CHAR(MINUTE(login_timestamp), '00')) AS hour_minute
              , MIN(DATE(login_timestamp)) OVER (PARTITION BY user_name, HOUR(login_timestamp), MINUTE(login_timestamp)) AS begin_t
              , MAX(DATE(login_timestamp)) OVER (PARTITION BY user_name, HOUR(login_timestamp), MINUTE(login_timestamp)) AS end_t
              , COUNT(user_name) OVER (PARTITION BY user_name, HOUR(login_timestamp), MINUTE(login_timestamp)) AS num_failures
           FROM v_monitor.login_failures
          WHERE login_timestamp >= CURRENT_TIMESTAMP - (SELECT current_value 
                                                          FROM v_internal.vs_tuning_rule_parameters
                                                         WHERE tuning_rule = 'user_r9'
                                                           AND parameter = 'days_to_check')::INTERVAL DAY) AS recurring_failures
  WHERE end_t - begin_t >= (SELECT current_value 
                              FROM v_internal.vs_tuning_rule_parameters
                             WHERE tuning_rule = 'user_r9'
                               AND parameter = 'attempts')::INTEGER ;

--Finds repeated login attempts.
--A repeated login attempt had more than 10 signon attempts per day within the last 14 days.
CREATE TUNING RULE user_r10 (LOGIN_FAILURES, REPEAT_ATTEMPTS, days_to_check=14, attempts=10) AS
SELECT NULL AS time
      ,'User ''' || user_name || ''' had ' || num_failures || ' failed signon attempts' || ' ON ' || login_date || '.' AS observation_description
      , NULL AS table_id
      , NULL AS transaction_id
      , NULL AS statement_id
      , 'SELECT * FROM v_monitor.login_failures WHERE user_name = ''' || user_name || ''';' AS tuning_parameter
      , 'Review excessive sign-in failures that occur in a short timeframe. Failure Reason: ' || reason AS tuning_description
      , 'Investigate the source of the login failures and correct as necessary.'  AS tuning_command
      , 'MEDIUM' AS tuning_cost
  FROM (SELECT user_name, reason, DATE(login_timestamp) AS login_date, COUNT(user_name) AS num_failures
          FROM v_monitor.login_failures
         WHERE login_timestamp >= CURRENT_TIMESTAMP - (SELECT current_value 
                                                         FROM v_internal.vs_tuning_rule_parameters
                                                        WHERE tuning_rule = 'user_r10'
                                                          AND parameter = 'days_to_check')::INTERVAL DAY
         GROUP BY user_name, reason, DATE(login_timestamp)
        HAVING COUNT(user_name) >=  (SELECT current_value 
                                       FROM v_internal.vs_tuning_rule_parameters
                                      WHERE tuning_rule = 'user_r10'
                                        AND parameter = 'attempts')::INTEGER) lf ;

--Finds duplicate projections.
--A duplicate projection exists on the same table and has the same first and last sort value, the same number of columns in the sort key
--and the same segmentation value.
CREATE TUNING RULE user_r11 (PROJECTIONS, DUPLICATE) AS
SELECT CURRENT_TIME AS time
     , 'Table ' || projection_schema || '.' || tablename || ' appears to have ' || _count-1 || ' or more duplicate projections. Review projection definitions and drop duplicate projections.'  AS observation_description
     , table_id AS table_id
     , tablename AS table_name
     , schema_id AS table_schema_id
     , projection_schema AS table_schema
     , NULL AS transaction_id
     , NULL AS statement_id
     , NULL AS tuning_parameter
     , 'Duplicate projection definition found.' AS tuning_description
     , 'SELECT EXPORT_OBJECTS('''', '''  || projection_schema || '.' || tablename || ''');' AS tuning_command
     , 'MEDIUM' AS tuning_cost
  FROM ( SELECT projection_schema, tablename, column_count, first_sort, last_sort, hash_segment, table_id, schema_id
              , count(*) AS _count
           FROM (SELECT DISTINCT projection_schema, p.anchor_table_name AS tablename, p.projection_basename
                      , COUNT(projection_column_name) OVER (w) AS column_count
                      , FIRST_VALUE(projection_column_name) OVER (w ORDER BY sort_position ASC) AS first_sort
                      , FIRST_VALUE(projection_column_name) OVER (w ORDER BY sort_position DESC) AS last_sort
                      , HASH(segment_expression) AS hash_segment
                      , t.table_id, s.schema_id
                   FROM v_catalog.projections p JOIN projection_columns pc USING (projection_id)
                   JOIN v_catalog.tables t ON p.anchor_table_id = t.table_id
                   JOIN v_catalog.schemata s ON p.projection_schema_id = s.schema_id
                  WHERE p.is_segmented AND pc.sort_position IS NOT NULL
                    AND NOT t.is_flextable
                    AND NOT t.is_system_table
                 WINDOW w AS (PARTITION BY p.projection_name) ) AS foo
 GROUP BY 1,2,3,4,5,6,7,8 
HAVING COUNT(*) > 1) AS bar ;

--Finds projection delete concerns and reports on them. Of course, the user would still have to run evaluate_delete_performance in order to generate results here
CREATE TUNING RULE user_r12 (PROJECTIONS, DELETE_CONCERNS) AS
SELECT CURRENT_TIME AS time
     , 'Projection ' || s.schema_name || '.' || pdc.projection_name || ' has delete/update performance concerns. Review projection definition. The projection sort order may need revision.'  AS observation_description
     , t.table_id AS table_id
     , t.table_name AS table_name
     , s.schema_id AS table_schema_id
     , s.schema_name AS table_schema
     , NULL AS transaction_id
     , NULL AS statement_id
     , NULL AS tuning_parameter
     , 'Update/Delete performance concerns found.' AS tuning_description
     , 'SELECT * FROM projection_delete_concerns ;' AS tuning_command
     , 'MEDIUM' AS tuning_cost
  FROM v_catalog.projection_delete_concerns pdc
  JOIN v_catalog.schemata s ON pdc.projection_schema = s.schema_name
  JOIN v_catalog.projections p USING (projection_id)
  JOIN v_catalog.tables t ON t.table_id = p.anchor_table_id ;


--observation: data distribution in segmented projection is skewed
--condition:   projection is up_to_date & segmented on all nodes
--recommendation: alter segmentation expression in the projection
--"user_r13"
CREATE TUNING RULE user_r13 (PROJECTIONS, SKEW, min_avg_row_count=100000, skew_pct=20) AS
SELECT CURRENT_TIME AS time
     , 'Data distribution in segmented projection ' || projection_schema || '.' || projection_name || ' is ' || TO_CHAR(100-(skew_pct*100.0), '99.9') || '% skewed' AS observation_description
     , anchor_table_id AS table_id
     , anchor_table_name AS table_name
     , projection_schema_id AS table_schema_id
     , projection_schema AS table_schema
     , NULL AS transaction_id
     , NULL AS statement_id
     , NULL AS tuning_parameter
     , 'Re-segment projection ' || projection_schema || '.' || projection_name || ' on high-cardinality column(s)' AS tuning_description
     , NULL AS tuning_command
     , 'MEDIUM' AS tuning_cost
  FROM (SELECT DISTINCT ps.projection_schema, p.projection_schema_id, ps.projection_name
             , p.anchor_table_id, p.anchor_table_name
             , FIRST_VALUE(used_bytes) OVER (w ORDER BY used_bytes ASC) AS min_used_bytes
             , FIRST_VALUE(used_bytes) OVER (w ORDER BY used_bytes DESC) AS max_used_bytes
             , FIRST_VALUE(used_bytes) OVER (w ORDER BY used_bytes ASC) /
               FIRST_VALUE(used_bytes) OVER (w ORDER BY used_bytes DESC) AS skew_pct
             FROM (SELECT node_name, projection_id, projection_schema, projection_name
                        , SUM(used_bytes) AS used_bytes, AVG(row_count) AS avg_row_count
                     FROM v_monitor.projection_storage GROUP BY 1,2,3,4 ) AS ps
             JOIN v_catalog.projections p USING (projection_id)
            WHERE p.is_segmented
              AND ps.used_bytes > 0
              AND avg_row_count > (SELECT current_value
                                     FROM v_internal.vs_tuning_rule_parameters
                                    WHERE tuning_rule = 'user_r13'
                                      AND parameter = 'min_avg_row_count')
           WINDOW w AS (PARTITION BY ps.projection_schema, ps.projection_name)) AS foo
 WHERE 100-(skew_pct*100.0)  > (SELECT current_value
                                  FROM v_internal.vs_tuning_rule_parameters
                                 WHERE tuning_rule = 'user_r13'
                                   AND parameter = 'skew_pct') ;

--Finds partitioned tables that are getting close (80%) to the MaxPartitionCount system parameter.
--approximate_count_distinct is probably good enough here. Is about 2x as fast
CREATE TUNING RULE user_r14 (PARTITIONS, TABLES, partition_threshold_pct=80) AS
SELECT DISTINCT CURRENT_TIME AS time
     , 'Table ' || schema_name || '.' || table_name || ' has ' || num_partitions || ' partitions which is reaching the maximum number allowable of ' || max_parts.current_value || '.' AS observation_description
     , table_id AS table_id
     , table_name AS table_name
     , schema_id AS table_schema_id
     , schema_name AS table_schema
     , NULL AS transaction_id
     , NULL AS statement_id
     , NULL AS tuning_parameter
     , 'Choose a less granular partition key for this table, or else increase MaxPartitionCount and/or ContainersPerProjectionLimit' AS tuning_description
     , NULL AS tuning_command
     , 'HIGH' AS tuning_cost
  FROM (SELECT s.schema_name, s.schema_id
             , t.table_name, t.table_id
             , APPROXIMATE_COUNT_DISTINCT(part.partition_key) AS num_partitions
          FROM v_monitor.partitions part
          JOIN v_catalog.projections p USING (projection_id)
          JOIN v_catalog.tables t ON p.anchor_table_id = t.table_id
          JOIN v_catalog.schemata s ON t.table_schema_id = s.schema_id
         WHERE NOT t.is_system_table
           AND NOT t.is_temp_table
         GROUP BY 1,2,3,4) AS parts
  JOIN (SELECT current_value
          FROM v_monitor.configuration_parameters
         WHERE parameter_name = 'MaxPartitionCount') AS max_parts ON 1=1
 WHERE num_partitions > max_parts.current_value::INTEGER *
                      ((SELECT current_value
                          FROM v_internal.vs_tuning_rule_parameters
                         WHERE tuning_rule = 'user_r14'
                           AND parameter = 'partition_threshold_pct')::INTEGER / 100)  ;


--finds backups which haven't ran since 2x as long as their normal backup interval. Accounts for weekly backups, for example.
CREATE TUNING RULE user_r15 (BACKUP, SYSTEM) AS
SELECT DISTINCT CURRENT_TIME AS time
     , 'Backup ''' || snapshot_name || ''' which would normally run every ' || backup_interval || ' days, has not ran in ' || date(now()) - last_backup || ' days. Please take corrective action.' AS observation_description
     , null AS table_id
     , null AS table_name
     , null AS table_schema_id
     , null AS table_schema
     , NULL AS transaction_id
     , NULL AS statement_id
     , NULL AS tuning_parameter
     , 'Check on the status of the backups' AS tuning_description
     , NULL AS tuning_command
     , 'MEDIUM' AS tuning_cost
from ( select a.snapshot_name, round(avg(diff)) as backup_interval
            , max(date("time")) as last_backup
         from ( select snapshot_name
                     , date("time") - lag(date("time")) over (partition by snapshot_name order by "time") as diff
                  from dc_backups ) a
         join dc_backups on a.snapshot_name = dc_backups.snapshot_name
        group by 1 ) b
where date(now()) - last_backup > backup_interval * 2 ;
	
