In-Flight Query Diagnostics
Scenario
In SQLDW, there are two types of query performance troubleshooting.
•	Telemetry based troubleshooting. For SQLDW, this is mainly based on Kusto query and its variances. It is always on and can be done without user session.
•	User facing in-flight query diagnostics. In this case, the diagnostics is done through user sessions, with the help of customer and field engineers etc, such as POC, or some customer engagement and this is what this TSG is about

During user facing in-flight query performance diagnostics, sometimes it is needed to capture the physical plan information from compute nodes. Currently, SQLDW has several mechanisms for to get the physical plans for different scenarios,
1.	Capture all physical plans from all compute nodes for SQL query and data movement after a query is completed.
•	Run: set query_diagnostics on to turn on the session variable.
2. Execute problematic query **(please use sqlcmd with -y0 option to avoid truncated plans)**
•	The query will now return a result set of the plans from each distribution after each optimizable step. If the step is not optimizable (i.e., if it does not go through QO) no plans will be returned for that step.
Example: Sqlcmd -S "dwtestsvrsouthcentralus.database.windows.net" -y0 -d TPCDS_10TB -U cloudSA_xlargerc -P dogmat1C -I -i .\GetQueryPlan.txt -o .\q67_output.txt
•	The following Example is based on query diagnostics equal on and is executed within the session. 
 
 
 ![image](https://user-images.githubusercontent.com/91505344/196849694-9b91243a-43a0-4897-a29e-b723fedf8588.png)

set query_diagnostics on
•	Run: set query_diagnostics off to turn off the session variable.

2.	 This is the scenario when a query can be completed. 
3.	This is the scenario when there is a long running query in a specific distribution(s). The captured plan is the estimated plan.
3. In T46, 5 DW passed through DMVs have been added to improve in-flight query performance diagnostics. This is also for debugging long running queries stuck in certain steps.
o	dm_pdw_nodes_exec_sql_text
o	dm_pdw_nodes_exec_query_plan
o	dm_pdw_nodes_exec_query_profiles
o	dm_pdw_nodes_exec_query_statistics_xml
o	dm_pdw_nodes_exec_text_query_plan
These DMVs has similar DMV or DMF in SQL server. But in DW, they are all exposed as passed through DMVs. Reference these links for the details of these DMVs
*Note:*

1. The amount of data returned from these DMVs can be large for a system with many query. So it is important to identify the stuck step, and drill down to node and session to query these DMVs.*
2. Example of trouble shooting. You can change the steps to whatever more helpful.
select * from sys.dm_pdw_exec_requests
where status = 'Running'
order by submit_time desc

This is to identify the query which is needs to be troubleshoot, QID1463796in this case it is Query75 in TPC-DS.
![image](https://user-images.githubusercontent.com/91505344/196849731-221f66d0-b072-458e-9670-6863f8d58750.png)
 
 
To identify the step:
select * from sys.dm_pdw_request_steps
where request_id = 'QID1463796'
Step_index 15 is the step.
![image](https://user-images.githubusercontent.com/91505344/196849762-788bb143-a3df-4cd4-a7d9-ce4befeb90fc.png)
  
select * from sys.dm_pdw_sql_requests
where request_id = 'QID1463796'
and step_index = 15
![image](https://user-images.githubusercontent.com/91505344/196849787-9e99f474-c4cb-4f2e-9b66-e47691bac052.png)
 
 
Assume we need to trouble shoot spid 599
Note:
Depending on the step, spid can be retrieved from sys.dm_pdw_sql_requests or sys.dm_pdw_dms_workers
Here is an example using sys.dm_pdw_dms_workers
select pdw_node_id, distribution_id, sql_spid, *
from sys.dm_pdw_dms_workers
where request_id = N'<QID>'
and step_index = 23
and [type] like '%READER%';
You can collect information from 1 dmv or multiple dmv. Here just give an example to collect all information
declare @pdw_node_id int = <node>
declare @session_id nvarchar(32) = <session>
select * from sys.dm_pdw_nodes_exec_query_plan
Where session_id = @session_id and pdw_node_id = @pdw_node_id
select * from sys.dm_pdw_nodes_exec_sql_text
Where session_id = @session_id and pdw_node_id = @pdw_node_id
select * from sys.dm_pdw_nodes_exec_query_statistics_xml
Where session_id = @session_id and pdw_node_id = @pdw_node_id
select * from sys.dm_pdw_nodes_exec_query_profiles
Where session_id = @session_id and pdw_node_id = @pdw_node_id
select * from
Where session_id = @session_id and pdw_node_id = @pdw_node_id
Because there is limitation SSMS truncates the output, it is recommended use sqlcmd with -y0 option to get it.
sqlcmd -S dwtestsvrscus.database.windows.net -d DwPerformanceTest -U cloudsa -P dogmat1C -I -i .\captureall.sql -o .\all.txt -y0
all.txt
Captureall.sql:

declare @pdw_node_id int = 14
declare @session_id int = 801

select * from sys.dm_pdw_nodes_exec_query_plan
where pdw_node_id = @pdw_node_id and session_id = @session_id

select * from sys.dm_pdw_nodes_exec_sql_text
where pdw_node_id = @pdw_node_id and session_id = @session_id
 

