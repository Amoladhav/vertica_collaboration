-- *****************
-- Cluster State
-- ****************

-- Vertica license 
select get_compliance_status();

-- Node Dependencies 
select get_node_dependecies(); 

-- Node State
SELECT node_name, node_state FROM nodes WHERE node_state != 'UP' ORDER BY 1;

-- Spread Retransmit 
SELECT a."time", a.node_name, a.retrans, a.time_interval, a.packet_count,
        ((a.retrans / (a.time_interval / '00:00:01'::interval)))::numeric(18,2) AS retrans_per_second
    FROM (
        SELECT (dc_spread_monitor."time")::timestamp AS "time", dc_spread_monitor.node_name,
            (dc_spread_monitor.retrans - lag(dc_spread_monitor.retrans, 1, NULL::int) OVER
                (PARTITION BY dc_spread_monitor.node_name ORDER BY (dc_spread_monitor."time")::timestamp)) AS retrans,
            (((dc_spread_monitor."time")::timestamp - lag((dc_spread_monitor."time")::timestamp, 1, NULL::timestamp) OVER
                (PARTITION BY dc_spread_monitor.node_name ORDER BY (dc_spread_monitor."time")::timestamp))) AS time_interval,
            (dc_spread_monitor.packet_sent - lag(dc_spread_monitor.packet_sent, 1, NULL::int) OVER
                (PARTITION BY dc_spread_monitor.node_name ORDER BY (dc_spread_monitor."time")::timestamp)) AS packet_count
        FROM v_internal.dc_spread_monitor
    ) a ORDER BY a."time", a.node_name

-- Too many errors 
-- Catalog size in memory
SELECT node_name
	,max(ts) AS ts
	,max(catalog_size_in_MB) AS catlog_size_in_MB
FROM (
	SELECT node_name
		,trunc((dc_allocation_pool_statistics_by_second."time")::TIMESTAMP, 'SS'::VARCHAR(2)) AS ts
		,sum((dc_allocation_pool_statistics_by_second.total_memory_max_value - dc_allocation_pool_statistics_by_second.free_memory_min_value)) / (1024 * 1024) AS catalog_size_in_MB
	FROM dc_allocation_pool_statistics_by_second
	GROUP BY 1
		,trunc((dc_allocation_pool_statistics_by_second."time")::TIMESTAMP, 'SS'::VARCHAR(2))
	) foo
GROUP BY 1
ORDER BY 1 limit 50;

-- Storage Usage
SELECT node_name
	,storage_path
	,disk_space_free_percent
	,disk_space_used_mb
	,storage_status
FROM disk_storage
WHERE substr(disk_space_free_percent, 0, 3) < 20;

--********************
-- Database Operations
--********************

-- Long Mergeouts 
SELECT a.node_name
	,a.schema_name
	,b.projection_name
	,count(*)
FROM dc_tuple_mover_events a
	,${ schema}.dc_tuple_mover_events b
WHERE a.transaction_id = b.transaction_id
	AND a.event = 'Start'
	AND b.event = 'Complete'
	AND a.TIME::TIMESTAMP BETWEEN '${start_time}'
		AND '${end_time}'
	AND b.TIME::TIMESTAMP - a.TIME::TIMESTAMP > interval '10 minutes'
GROUP BY 1
	,2
	,3
ORDER BY 4 DESC;

-- Long reply deletes

SELECT a.node_name
	,a.schema_name
	,b.projection_name
	,count(*)
FROM ${ schema}.dc_tuple_mover_events a
	,${ schema}.dc_tuple_mover_events b
WHERE a.transaction_id = b.transaction_id
	AND a.event = 'Change plan type to Replay Delete'
	AND b.event = 'Complete'
	AND a.TIME::TIMESTAMP BETWEEN '${start_time}'
		AND '${end_time}'
	AND b.TIME - a.TIME > interval '10 minutes'
GROUP BY 1
	,2
	,3
ORDER BY 4 DESC;

-- AHM no advancing
SELECT get_ahm_time(), get_ahm_epoch(), get_last_good_epoch(), get_current_epoch(), sysdate;

-- ROS counts
SELECT node_name,
     projection_schema,
     projection_name,
     SUM(ros_count)
AS ros_count
FROM v_monitor.projection_storage
GROUP BY node_name,
     projection_schema,
     projection_name
ORDER BY ros_count DESC;

-- Statistics no collected
SELECT projection_name
	,count(*) AS invalid_projections
	,anchor_table_name
	,owner_name
FROM projections
WHERE is_up_to_date = 'false'
GROUP BY projection_name
	,anchor_table_name
	,owner_name;

--********************
-- Resource Usage 
--********************

--Delete vectors
SELECT COUNT(*) FROM v_monitor.delete_vectors;

-- Open Sessions
SELECT sessions.session_id
	,sessions.node_name
	,sessions.transaction_id
	,sessions.statement_id
	,(((statement_timestamp())::TIMESTAMP - sessions.transaction_start)) AS running_time
	,sessions.user_name
	,substr(sessions.current_statement, 1, 120) AS current_statement
	,substr(sessions.last_statement, 1, 50) AS last_statement
FROM v_monitor.sessions
ORDER BY (((statement_timestamp())::TIMESTAMP - sessions.transaction_start)) DESC;

--Rejections
select * from resource_rejections;
--Query Events
SELECT event_type
	,event_category
	,event_details
	,COUNT(DISTINCT node_name) num_nodes
	,count(DISTINCT transaction_id) num_transactions
	,COUNT(*)
FROM query_events
GROUP BY 1
	,2
	,3
ORDER BY 2;

--Too many errors 
SELECT date_trunc('hour', TIME)
	,node_name
	,user_name
	,error_level_name
	,function_name
	,count(*)
FROM dc_errors
GROUP BY 1
	,2
	,3
	,4
	,5
HAVING count(*) > 10;

--Statistics no collected 
SELECT projection_name
	,count(*) AS invalid_projections
	,anchor_table_name
	,owner_name
FROM projections
WHERE is_up_to_date = 'false'
GROUP BY projection_name
	,anchor_table_name
	,owner_name;

--Projections no used ( Loaded by not query) 
SELECT anchor_table_schema
	,anchor_table_name
	,projection_name
	,max(query_start_timestamp) last_loaded
FROM projection_usage
WHERE io_type = 'output'
	AND projection_id NOT IN (
		SELECT projection_id
		FROM projection_usage
		WHERE io_type = 'input'
		)
	AND anchor_table_id IN (
		SELECT anchor_table_id
		FROM projections
		WHERE is_segmented
		GROUP BY 1
		HAVING count(*) > 2
		)
GROUP BY 1
	,2
	,3
ORDER BY 1
	,2
	,3;

--Long running queries 
SELECT user_name
	,session_id
	,node_name
	,start_timestamp
	,statement_id
FROM query_requests
WHERE end_timestamp IS NULL
	AND (to_char(sysdate, 'MI') - to_char(start_timestamp, 'MI')) > 1440;

--Resource Usage
SELECT resource_pool_status.node_name
	,resource_pool_status.pool_name
	,round((resource_pool_status.memory_size_actual_kb / 1048576::FLOAT), 0) AS size_gb
	,round((resource_pool_status.memory_inuse_kb / 1048576::FLOAT), 3) AS used_gb
	,round((resource_pool_status.general_memory_borrowed_kb / 1048576::FLOAT), 3) AS borred_gral_gb
	,resource_pool_status.running_query_count
	,round((resource_pool_status.query_budget_kb / 1048576::FLOAT), 3) AS query_budget_gb
FROM v_monitor.resource_pool_status;


-- ******************
-- Node Health
-- ******************


--High load average
uptime 
top


--Network errors
nestat -x 
netstat -s | grep error
--Swap Space 
free -t 
top
vmstat 

--Verify that Operating System are alright
$ /opt/vertica/oss/python/bin/python -m vertica.local_verify











- 