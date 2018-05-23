select 
case 
--system messages
when substr(message, 1, 20) = 'Could not link file ' then 'File linkage error; caused from upgrade'
when message ilike 'Node%Non-existent snapshot%' then 'Copycluster Warning'

--Tuple mover messages
when substr(message, 1, 20) = 'A Mergeout operation' then 'Mergeout was already running'
when substr(message, 1, 19) = 'A Moveout operation' then 'Moveout was already running'

--User errors and canceled requests
when substr(message, 1, 30) = 'Execution canceled by operator' then 'User canceled request'
when substr(message, 1, 23) = 'Client canceled session' then 'User canceled request'
when message = 'Plan canceled prior to execute call' then 'User canceled request'
when message ilike 'Client error: Could not open file%' then 'User error'
when message ilike 'Client error:%' then 'User error'
when message = 'Meta-functions cannot be used in directed queries' then 'User error'
when message ilike 'Processing aborted by peer %' then 'User canceled request'
when message ilike 'Execution canceled%' then 'User canceled request'
when message ilike 'Catalog object%does not exist' then 'User error'
when message ilike 'Database%does not exist' then 'User error'
when message ilike 'Projection%is not available for query processing%' then 'User error'

--query execution errors
when message = 'Join did not fit in memory' then 'Join spill error'
when substr(message, 1, 30) = 'Join inner did not fit in memo' then 'Join spill error'

--data quality issues
when substr(message, 1, 30) = 'Float "nan" is out of range fo' then 'Data quality issue'
when message ilike '%octets is too long for type%' then 'Data quality issue'
when message = 'Subquery used as an expression returned more than one row' then 'Data quality issue'
when message ilike 'Cannot set a NOT NULL column%' then 'Data quality issue'
when message ilike '%is out of range as a float%' then 'Data quality issue'
when message ilike 'Value exceeds range of type%' then 'Data quality issue'
when message ilike 'Regexp encountered an invalid%character' then 'Data quality issue'

--out of memory & Resource pool constraint errors
when substr(message, 1, 20) = 'Unable to reserve me' then 'Out of memory'
when substr(message, 1, 30) = 'Insufficient resources to exec' then 'Out of memory'
when message ilike 'Plan memory limit exhausted%' then 'Out of memory'
when substr(message, 1, 30) = 'Query canceled while waiting f' then 'Query queue timeout'
when message ilike 'Execution time exceeded run time cap%' then 'runtime cap exceeded'
when message ilike '%inner partition did not fit in memory%' then 'Out of memory'

--node down messages
when substr(message, 1, 11) = 'Recceive on' then 'Server not responding'
when substr(message, 1, 30) = 'One or more nodes did not open' then 'Server not responding'
when substr(message, 1, 25) = 'Send: Connection not open' then 'Server not responding'
when substr(message, 1, 10) = 'Receive on' then 'Server not responding'

--signin and permissions errors
when message = 'Invalid username or password' then 'Signon error'
when substr(message, 1, 19) = 'LDAP authentication' then 'Signon error'
when message ilike 'Permission denied%' then 'Permission denied'

--Syntax errors
when substr(message, 1, 17) = 'Could not convert' then 'Syntax error'
when substr(message, 1, 9) = 'Relation ' then 'Syntax error'
when message ilike '%must appear in the GROUP BY%' then 'Syntax error'
when message ilike 'Schema%does not exist' then 'Syntax error'
when message ilike 'GROUP BY position%' then 'Syntax error'
when message ilike 'Column reference%is ambiguous%' then 'Syntax error'
when message ilike 'INSERT has more expressions%' then 'Syntax error'
when message ilike 'Object%already exists%' then 'Syntax error'
when message = 'Aggregates not allowed in GROUP BY clause' then 'Syntax error'
when message ilike 'Table%does not exist%' then 'Syntax error'
when message ilike 'Date/time field value out of range%' then 'Syntax error'
when message ilike 'Column%is of type %' then 'Syntax error'
when message ilike 'LIMIT%syntax is not supported%' then 'Syntax error'
when message ilike 'Unterminated quoted string at or near%' then 'Syntax error'
when message ilike 'Invalid syntax for float%' then 'Syntax error'
when message = 'Aggregates not allowed in WHERE clause' then 'Syntax error'
when substr(message, 1, 12) = 'Syntax error' then 'Syntax error'
when message ilike 'Column%does not exist' then 'Syntax error'
when message ilike 'Operator does not exist%' then 'Syntax error'
when message ilike 'Invalid input syntax for boolean%' then 'Syntax error'
when message = 'Subquery in FROM must have an alias' then 'Syntax error'
when message ilike 'Missing FROM-clause entry for table%' then 'Syntax error'
when message = 'Non-equality correlated subquery expression is not supported' then 'Syntax error'
when message ilike 'Type%does not exist' then 'Syntax error'
when message ilike 'Invalid input syntax for%' then 'Syntax error'
when message ilike 'Invalid number at or near%' then 'Syntax error'
when message = 'Argument of AND must be type boolean, not type varchar' then 'Syntax error'
when message ilike '%ORDER BY expressions must appear in the SELECT clause%' then 'Syntax error'
when message ilike '%types%are inconsistent%' then 'Syntax error'

--miscellaneous
when message ilike '%lock table%' then 'Table locked'
when message ilike 'Unavailable:%initiator locks for query - timeout error%' then 'Table locked'
when message ilike '%Input record%has been rejected%' then 'COPY error'
when message = 'Statement abandoned due to subsequent DDL' then 'DDL interrupted statement'
when message = 'DDL statement interfered with this statement' then 'DDL interrupted statement'
when message ilike 'Role%was already granted%' then 'Role already granted'
when message ilike 'Function%does not exist%' then 'Function does not exist'
when message ilike 'Invalid view%' then 'Invalid view definition'
when message ilike 'Error calling%in User Function%' then 'Error calling user function'
when message ilike 'Failed to read parquet source%' then 'Parquet file read error.'
when message ilike 'Error reading from Parquet parser%' then 'Parquet file read error.'
when message = 'Cannot commit; no transaction in progress' then 'Commit error; no transaction'
else substr(message, 1, 200) end as high_level_msg
, count(distinct transaction_id) num_transactions
, count(1)
from error_messages 
--where event_timestamp >= '2016-11-02 04:40:00' and event_timestamp <= '2016-11-02 05:30:00'
group by 1 having count(*) > 5
order by 3 desc ;
