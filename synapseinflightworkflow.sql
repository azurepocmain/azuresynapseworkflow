--STEP 1: 
-- First step is to confirm active queries to isolate user queries
-- Is the full text available? If so, check the estimated query plan by adding the EXPLAIN preceding the query. 
/* Has the query started or is it still pending, check for a start time and the status to confirm its not suspended which means resource allocation pending tasks. */
SELECT *
FROM sys.dm_pdw_exec_requests
WHERE status not in ('Completed','Failed','Cancelled')
AND session_id <> session_id()
ORDER BY submit_time DESC;

DECLARE @QIDINFO varchar(15) = 'QID615063'--<<<<--ADD_QID_HERE------
--STEP 2: 
--Identify the step that is still running and take note, replace with the QID from the prior step.
-- Do you see more than two steps? It may be an indication that the table distribution is not optimized for this query. Verify if you can provide the distribution column to the user to add to their query. Or consider redistributing the table if this is a continual issue. 
/*Confirm the operation type that is running, and command, if elapse time is long, we need to drill down to confirm if a particular distribution is causing this. */
-- Check the row count as well.
SELECT *
FROM sys.dm_pdw_request_steps
WHERE request_id = @QIDINFO --Place your request_id here from the prior step. 
ORDER BY step_index;


--STEP 3: 
/*Verify why a step is taking longer on a specific compute node, review if the total time and if a compute node is running longer than others */
SELECT * FROM sys.dm_pdw_sql_requests
WHERE request_id = @QIDINFO --Place your request_id here
AND step_index = <number> --Place your step_index ID here
order by spid;

--STEP 3a: 
--You can also get a glimpse of the table joins by using the below query and reviewing the join operation and select statement metadata. 
select * from 	sys.dm_pdw_nodes_exec_text_query_plan where pdw_node_id=<id> and session_id=<session number>


--STEP 4: 
--To get the plan for the distributed query run the following.
--Check the join operation on the long running query, does it look valid. 
--Confirm if there are any invalid joins or if the estimated rows are off.
--This can be an indication of statistic issues.
DBCC PDW_SHOWEXECUTIONPLAN (distribution_id, spid)
--Save the plan as .sqlplan to review the execution plan.


--STEP 5: 
--If the steps are just returning results, confirm that the client is just not taking a long time to process 
--the results. If executing, confirm if there is a particular move operation that is taking the most duration. 
select * from sys.dm_pdw_nodes_os_waiting_tasks  where session_id IN (SELECT spid
FROM sys.dm_pdw_sql_requests
WHERE request_id = @QIDINFO --Place your request_id here
AND step_index = <number> --Place your step_index ID here);

--STEP 6: 
--If the session is in suspended state, verify if the issue is regarding concurrency slots.
--Remember, different concurrency slots are aggregated to equal 100% of the overall concurrency slots available for that DWUc.
--It’s important to also note the maximum concurrent queries for each DWUc.
--We need to balance concurrency and performance of queries. This is where importance levels are impactful for important workloads. 
SELECT waits.session_id,
      waits.request_id, 
      requests.command,
      requests.status,
      requests.start_time, 
      waits.type,
      waits.state,
      waits.object_type,
      waits.object_name
FROM   sys.dm_pdw_waits waits
   JOIN sys.dm_pdw_exec_requests requests
   ON waits.request_id=requests.request_id
ORDER BY waits.object_name, waits.object_type, waits.state;


--STEP 7: 
--Verify if there are any table skewed in a specific distribution.
SELECT *
FROM sys.dm_pdw_dms_workers
WHERE request_id = @QIDINFO --Place your request_id here
AND step_index = <stepIDHere>; --Place your step_index ID here



--STEP 8: 
--Confirm that the SQL did not have errors:
select * From sys.dm_pdw_errors where request_id=@QIDINFO;


--STEP 9: 
--Verify the object name of the request:
SELECT waits.session_id, waits.request_id, requests.command,
requests.status, requests.start_time, waits.type, waits.state,
waits.object_type, waits.object_name
FROM   sys.dm_pdw_waits waits
JOIN  sys.dm_pdw_exec_requests requests
ON waits.request_id=requests.request_id
WHERE waits.request_id = @QIDINFO
ORDER BY waits.object_name, waits.object_type, waits.state;
