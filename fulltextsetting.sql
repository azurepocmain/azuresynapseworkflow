--Currently, sys.dm_pdw_exec_requests has two columns to retrieve full query text. The command columns and the command2 column. 
--Any query text submitted by the user that is under 4000 characters will appear in the command column. 
--Any query over 4000 characters will appear in the command2 column. 
--If the data is getting purged relatively faster than you would like. 
--You can consider enabling query store to get the full text. However, there is overhead with enabling it and should only be enabled during the time of diagnostic and disabled after.

SELECT *
FROM sys.dm_pdw_exec_requests
order by submit_time desc;
--Reference: https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-pdw-exec-requests-transact-sql?view=aps-pdw-2016-au7


***Only enable during diagnostics as it causes overhead***
--Verify the current state of Query Store in Synapse SQL Pool.
select * from sys.database_query_store_options;


--If desired state set to "0 = OFF" consider enabling it. 
ALTER DATABASE [db_name_here]
SET QUERY_STORE = ON;


--Verify new state of Query Store. 
select * from sys.database_query_store_options;


--Get full query text. Remember that the query must be completed to show up in Query Store.
SELECT Txt.query_text_id, Txt.query_sql_text, Pl.plan_id, Qry.*
FROM sys.query_store_plan AS Pl
INNER JOIN sys.query_store_query AS Qry
    ON Pl.query_id = Qry.query_id
INNER JOIN sys.query_store_query_text AS Txt
    ON Qry.query_text_id = Txt.query_text_id
	order by Qry.last_execution_time desc;
	
--Disable Query Store. 	
ALTER DATABASE [db_name_here]
SET QUERY_STORE = OFF;
