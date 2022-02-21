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
