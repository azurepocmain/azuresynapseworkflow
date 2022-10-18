try {




    $dwdb=Get-AutomationVariable -Name 'Log_Analytics_test_synapse_database_name'
    $SQLDW=Get-AutomationVariable -Name 'Log_Analytics_test_synapse_instance_name'
    $workspaceidsynapse=Get-AutomationVariable -Name 'Log_Analytics_test_synapse_workspaceidsynapse'
    $workspacekeysynapse=Get-AutomationVariable -Name 'Log_Analytics_test_synapse_workspacekeysynapse'



###Context no longer needed as we will get the Synapse SQL Pool instance name from the config parameter.###

### Set-AzContext -SubscriptionId $env:azpocsub

#$SQLDW=@($env:AzureSynapse1);


##You can remove the below in Prod if you like after testing#####

Write-Host $SQLDW



##Write-Host $env:azpocsub

################################################

 

###You can use a foreach loop if there are multiple SQL DWs that require querying, you will have to set the instance and DB for every foreach call###

###The below is using managed identity of the Azure Function, ensure correct permissions is provided to the function in the GRANT VIEW DATABASE STATE TO [functionnamehere]###

###Calls to synapse DW should not incur any concurrency slots of resource usage when quiring DMVs#####Please note that microsoft.vw_sql_requests is required: https://github.com/Microsoft/sql-data-warehouse-samples/blob/main/solutions/monitoring/scripts/views/microsoft.vw_sql_requests.sql
##Please note that microsoft.vw_sql_requests is required: https://github.com/Microsoft/sql-data-warehouse-samples/blob/main/solutions/monitoring/scripts/views/microsoft.vw_sql_requests.sql

 


$resourceURI = "https://database.windows.net/"

$tokenAuthURI = $env:MSI_ENDPOINT + "?resource=$resourceURI&api-version=2017-09-01"

$tokenResponse = Invoke-RestMethod -Method Get -Headers @{"Secret"="$env:MSI_SECRET"} -Uri $tokenAuthURI

$accessToken = $tokenResponse.access_token

$SqlConnection = New-Object System.Data.SqlClient.SqlConnection

$SqlConnection.ConnectionString = "Server=tcp:$SQLDW,1433;Persist Security Info=False;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Initial Catalog=$dwdb;"

$SqlConnection.AccessToken = $AccessToken

$SqlCmd = New-Object System.Data.SqlClient.SqlCommand

$SqlCmd.CommandText = "SELECT Count(1) AS TOTAL `
FROM sys.dm_pdw_nodes_db_session_space_usage AS ssu `
INNER JOIN sys.dm_pdw_nodes_exec_sessions AS es ON ssu.session_id = es.session_id AND ssu.pdw_node_id = es.pdw_node_id `
INNER JOIN sys.dm_pdw_nodes_exec_connections AS er ON ssu.session_id = er.session_id AND ssu.pdw_node_id = er.pdw_node_id `
INNER JOIN ( `
SELECT `
sr.request_id, `
sr.step_index, `
(CASE WHEN (sr.distribution_id = -1 ) THEN (SELECT pdw_node_id FROM sys.dm_pdw_nodes WHERE type = 'CONTROL') ELSE d.pdw_node_id END) AS pdw_node_id, `
sr.distribution_id, `
sr.status, `
sr.error_id, `
sr.start_time, `
sr.end_time, `
sr.total_elapsed_time, `
sr.row_count, `
sr.spid, `
sr.command `
FROM `
sys.pdw_distributions AS d `
RIGHT JOIN sys.dm_pdw_sql_requests AS sr ON d.distribution_id = sr.distribution_id `
) AS sr ON ssu.session_id = sr.spid AND ssu.pdw_node_id = sr.pdw_node_id `
LEFT JOIN sys.dm_pdw_exec_requests exr on exr.request_id = sr.request_id `
LEFT JOIN sys.dm_pdw_exec_sessions exs on exr.session_id = exs.session_id `
WHERE `
DB_NAME(ssu.database_id) = 'tempdb' `
AND es.session_id <> @@SPID `
AND exs.session_id <> session_id() `
AND es.login_name <> 'sa' `
AND exs.login_name <> 'System' `
AND es.is_user_process = 1 `
AND exr.[end_time] IS NULL; "

$SqlCmd.Connection = $SqlConnection

$SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter

$SqlAdapter.SelectCommand = $SqlCmd

$dataset = New-Object System.Data.DataSet

$SqlAdapter.Fill($dataset)

$SqlConnection.Close()

$SynapseTempDB=($DataSet.Tables[0]).TOTAL



 


if ($SynapseTempDB -ge 1)

{

# Replace with your Workspace ID From Log Analytics

$CustomerId = $workspaceidsynapse

 

# Replace with your Primary Key From Log Analytics

$SharedKey = $workspacekeysynapse

 

# Specify the name of the record type that you'll be creating For This case it is Synapse Session info which will create a SynapseTempDBDW table in the workspace to query

$LogType = "SynapseTempDBDW"


# You can use an optional field to specify the timestamp from the data. If the time field is not specified, Azure Monitor assumes the time is the message ingestion time

$TimeStampField = ""



# The below metadata will be added to the workspace if the condition is met. There is an initial check above before this section executes to not waste resources

$resourceURI = "https://database.windows.net/"
$tokenAuthURI = $env:MSI_ENDPOINT + "?resource=$resourceURI&api-version=2017-09-01"
$tokenResponse = Invoke-RestMethod -Method Get -Headers @{"Secret"="$env:MSI_SECRET"} -Uri $tokenAuthURI
$accessToken = $tokenResponse.access_token
$SqlConnection = New-Object System.Data.SqlClient.SqlConnection
$SqlConnection.ConnectionString = "Server=tcp:$SQLDW,1433;Persist Security Info=False;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Initial Catalog=$dwdb;"
$SqlConnection.AccessToken = $AccessToken
$SqlCmd = New-Object System.Data.SqlClient.SqlCommand
$SqlCmd.CommandText = "SELECT `
exr.request_id `
,exr.session_id `
,exr.command `
,exr.[label] `
,exr.[status] `
,exr.[submit_time] `
,exr.[start_time] `
,exr.[end_time] `
,CONVERT(numeric(25,3),DATEDIFF(ms,exr.[submit_time],exr.[start_time]) / 1000.0) [Request_queue_time_sec]`
,CONVERT(numeric(25,3),DATEDIFF(ms,exr.[end_compile_time],exr.[start_time]) / 1000.0) AS [Request_compile_time_sec]`
,CONVERT(numeric(25,3),DATEDIFF(ms,exr.[end_compile_time],exr.[end_time]) / 1000.0) AS [Request_execution_time_sec]`
,CONVERT(numeric(25,2),exr.[total_Elapsed_time] / 1000.0) AS [Total_Elapsed_time_sec]`
,CONVERT(numeric(25,2),exr.[total_Elapsed_time] / 1000.0 / 60 ) AS [Total_Elapsed_time_min]`
,exs.login_name AS [loginName]`
,[MemoryUsage (in KB)] = SUM((es.memory_usage * 8)) `
,CONVERT(numeric(25,2),SUM((ssu.user_objects_alloc_page_count * 8)) / 1024.0) AS [Space_Allocated_For_User_Objects_MB]`
,CONVERT(numeric(25,2),SUM((ssu.internal_objects_alloc_page_count * 8)) / 1024.0) AS [Space_Allocated_For_Internal_Objects_MB] `
,[RowCount] = SUM(es.row_count) `
FROM sys.dm_pdw_nodes_db_session_space_usage AS ssu `
INNER JOIN sys.dm_pdw_nodes_exec_sessions AS es ON ssu.session_id = es.session_id AND ssu.pdw_node_id = es.pdw_node_id `
INNER JOIN sys.dm_pdw_nodes_exec_connections AS er ON ssu.session_id = er.session_id AND ssu.pdw_node_id = er.pdw_node_id `
INNER JOIN ( `
SELECT `
sr.request_id, `
sr.step_index, `
(CASE WHEN (sr.distribution_id = -1 ) THEN (SELECT pdw_node_id FROM sys.dm_pdw_nodes WHERE type = 'CONTROL') ELSE d.pdw_node_id END) AS pdw_node_id, `
sr.distribution_id, `
sr.status, `
sr.error_id, `
sr.start_time, `
sr.end_time, `
sr.total_elapsed_time, `
sr.row_count, `
sr.spid, `
sr.command `
FROM `
sys.pdw_distributions AS d `
RIGHT JOIN sys.dm_pdw_sql_requests AS sr ON d.distribution_id = sr.distribution_id `
) AS sr ON ssu.session_id = sr.spid AND ssu.pdw_node_id = sr.pdw_node_id `
LEFT JOIN sys.dm_pdw_exec_requests exr on exr.request_id = sr.request_id `
LEFT JOIN sys.dm_pdw_exec_sessions exs on exr.session_id = exs.session_id `
WHERE `
DB_NAME(ssu.database_id) = 'tempdb' `
AND es.session_id <> @@SPID `
AND exs.session_id <> session_id() `
AND es.login_name <> 'sa' `
AND exs.login_name <> 'System' `
AND es.is_user_process = 1 `
AND exr.[end_time] IS NULL `
GROUP BY `
exr.request_id `
,exr.session_id `
,exr.command `
,exr.[label] `
,exr.[status] `
,exr.[submit_time] `
,exr.[start_time] `
,exr.[end_time] `
,CONVERT(numeric(25,3),DATEDIFF(ms,exr.[submit_time],exr.[start_time]) / 1000.0) `
,CONVERT(numeric(25,3),DATEDIFF(ms,exr.[end_compile_time],exr.[start_time]) / 1000.0) `
,CONVERT(numeric(25,3),DATEDIFF(ms,exr.[end_compile_time],exr.[end_time]) / 1000.0) `
,CONVERT(numeric(25,2),exr.[total_Elapsed_time] / 1000.0) `
,CONVERT(numeric(25,2),exr.[total_Elapsed_time] / 1000.0 / 60 ) `
,exs.login_name;"

$SqlCmd.Connection = $SqlConnection
$SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
$SqlAdapter.SelectCommand = $SqlCmd
$dataset = New-Object System.Data.DataTable
$SqlAdapter.Fill($dataset)
$SqlConnection.Close()


###Convert the data to JSon directly and select the specific objects needed from the above query, all objects are selected in this case, but you can omit any if needed###

$SynapsePOC=$dataset | Select-Object request_id, loginName, session_id, submit_time,   start_time, end_time,  command,  Space_Allocated_For_User_Objects_MB,  Space_Allocated_For_Internal_Objects_MB, MemoryUsage,  RowCount, Total_Elapsed_time_min  |ConvertTo-Json






# Create the function to create the authorization signature

Function Build-Signature ($customerId, $sharedKey, $date, $contentLength, $method, $contentType, $resource)
{
$xHeaders = "x-ms-date:" + $date
$stringToHash = $method + "`n" + $contentLength + "`n" + $contentType + "`n" + $xHeaders + "`n" + $resource
$bytesToHash = [Text.Encoding]::UTF8.GetBytes($stringToHash)
$keyBytes = [Convert]::FromBase64String($sharedKey)
$sha256 = New-Object System.Security.Cryptography.HMACSHA256
$sha256.Key = $keyBytes
$calculatedHash = $sha256.ComputeHash($bytesToHash)
$encodedHash = [Convert]::ToBase64String($calculatedHash)
$authorization = 'SharedKey {0}:{1}' -f $customerId,$encodedHash
return $authorization
}



# Create the function to create and post the request

Function Post-LogAnalyticsData($customerId, $sharedKey, $body, $logType)

{
$method = "POST"
$contentType = "application/json"
$resource = "/api/logs"
$rfc1123date = [DateTime]::UtcNow.ToString("r")
$contentLength = $body.Length
$signature = Build-Signature `
-customerId $customerId `
-sharedKey $sharedKey `
-date $rfc1123date `
-contentLength $contentLength `
-method $method `
-contentType $contentType `
-resource $resource
$uri = "https://" + $customerId + ".ods.opinsights.azure.com" + $resource + "?api-version=2016-04-01"
 

$headers = @{
"Authorization" = $signature;
"Log-Type" = $logType;
"x-ms-date" = $rfc1123date;
"time-generated-field" = $TimeStampField;
}

 

$response = Invoke-WebRequest -Uri $uri -Method $method -ContentType $contentType -Headers $headers -Body $body -UseBasicParsing
return $response.StatusCode

 

}


# Submit the data to the API endpoint

Post-LogAnalyticsData -customerId $customerId -sharedKey $sharedKey -body ([System.Text.Encoding]::UTF8.GetBytes($SynapsePOC)) -logType $logType

}

} catch {

###########Catch Exception if there is an error###########

$Exception = $_.Exception.Message

###########Send Email of the exception###########

Write-Error -Exception $Exception


} finally {

###########Close any potential open connection###########

if ($SqlConnection.State -eq 'Open') {

$SqlConnection.Close()

}

}
