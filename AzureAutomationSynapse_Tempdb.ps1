 

try {




    $dwdb1=Get-AutomationVariable -Name 'Log_Analytics_test_synapse_database_name'
    $SQLDW=Get-AutomationVariable -Name 'Log_Analytics_test_synapse_instance_name'
    $workspaceidsynapse1=Get-AutomationVariable -Name 'Log_Analytics_test_synapse_workspaceidsynapse'
    $workspacekeysynapse=Get-AutomationVariable -Name 'Log_Analytics_test_synapse_workspacekeysynapse'



###Context no longer needed as we will get the Synapse SQL Pool instance name from the config parameter.###




##You can remove the below in Prod if you like after testing#####

Write-Host $SQLDW



##Write-Host $env:azpocsub

################################################

 

###You can use a foreach loop if there are multiple SQL DWs that require querying, you will have to set the instance and DB for every foreach call###

###The below is using managed identity of the Azure Function, ensure correct permissions is provided to the function in the GRANT VIEW DATABASE STATE TO [functionnamehere]###

###Calls to synapse DW should not incur any concurrency slots of resource usage when quiring DMVs###

 


 


$resourceURI = "https://database.windows.net/"

$tokenAuthURI = $env:MSI_ENDPOINT + "?resource=$resourceURI&api-version=2017-09-01"

$tokenResponse = Invoke-RestMethod -Method Get -Headers @{"Secret"="$env:MSI_SECRET"} -Uri $tokenAuthURI

$accessToken = $tokenResponse.access_token

$SqlConnection = New-Object System.Data.SqlClient.SqlConnection

$SqlConnection.ConnectionString = "Server=tcp:$SQLDW,1433;Persist Security Info=False;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Initial Catalog=$dwdb1;"

$SqlConnection.AccessToken = $AccessToken

$SqlCmd = New-Object System.Data.SqlClient.SqlCommand

$SqlCmd.CommandText = "SELECT `
 Count(1) AS TOTAL  `
FROM sys.dm_pdw_nodes_db_session_space_usage AS ssu `
    INNER JOIN sys.dm_pdw_nodes_exec_sessions AS es ON ssu.session_id = es.session_id AND ssu.pdw_node_id = es.pdw_node_id `
    INNER JOIN sys.dm_pdw_nodes_exec_connections AS er ON ssu.session_id = er.session_id AND ssu.pdw_node_id = er.pdw_node_id `
    --INNER JOIN microsoft.vw_sql_requests AS sr ON ssu.session_id = sr.spid AND ssu.pdw_node_id = sr.pdw_node_id `
	INNER JOIN sys.dm_pdw_exec_sessions exs on er.most_recent_session_id = exs.sql_spid `
    INNER JOIN sys.dm_pdw_exec_requests exr on exr.request_id = exs.request_id AND exr.session_id=exs.session_id `
WHERE DB_NAME(ssu.database_id) = 'tempdb' `
AND exr.end_time IS  NULL `
    AND es.session_id <> @@SPID `
    AND es.login_name <> 'sa'`
	AND  (ssu.user_objects_alloc_page_count * 8)  IS NOT NULL; "

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

$CustomerId = $workspaceidsynapse1

 

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

$SqlConnection.ConnectionString = "Server=tcp:$SQLDW,1433;Persist Security Info=False;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Initial Catalog=$dwdb1;"

$SqlConnection.AccessToken = $AccessToken

$SqlCmd = New-Object System.Data.SqlClient.SqlCommand

$SqlCmd.CommandText = "SELECT `
    exr.request_id, `
	exr.submit_time, `
    ssu.session_id, `
    ssu.pdw_node_id, `
    exr.command, `
    exr.start_time, `
	exr.end_time, `
    exs.login_name AS 'LoginName', `
    DB_NAME(ssu.database_id) AS 'DatabaseName', `
    (es.memory_usage * 8) AS 'MemoryUsage_in_KB', `
    (ssu.user_objects_alloc_page_count * 8) AS 'Space_Allocated_For_User_Objects_KB', `
    (ssu.user_objects_dealloc_page_count * 8) AS 'Space_Deallocated_For_User_Objects_KB', `
    (ssu.internal_objects_alloc_page_count * 8) AS 'Space_Allocated_For_Internal_Objects_KB', `
    (ssu.internal_objects_dealloc_page_count * 8) AS 'Space_Deallocated_For_Internal_Objects_KB', `
    CASE es.is_user_process `
    WHEN 1 THEN 'User Session' `
    WHEN 0 THEN 'System Session' `
    END AS 'SessionType', `
    es.row_count AS 'RowCount' `
FROM sys.dm_pdw_nodes_db_session_space_usage AS ssu `
    INNER JOIN sys.dm_pdw_nodes_exec_sessions AS es ON ssu.session_id = es.session_id AND ssu.pdw_node_id = es.pdw_node_id `
    INNER JOIN sys.dm_pdw_nodes_exec_connections AS er ON ssu.session_id = er.session_id AND ssu.pdw_node_id = er.pdw_node_id `
    --INNER JOIN microsoft.vw_sql_requests AS sr ON ssu.session_id = sr.spid AND ssu.pdw_node_id = sr.pdw_node_id `
	INNER JOIN sys.dm_pdw_exec_sessions exs on er.most_recent_session_id = exs.sql_spid `
    INNER JOIN sys.dm_pdw_exec_requests exr on exr.request_id = exs.request_id AND exr.session_id=exs.session_id `
WHERE DB_NAME(ssu.database_id) = 'tempdb' `
AND exr.end_time IS  NULL `
    AND es.session_id <> @@SPID `
    AND es.login_name <> 'sa'`
	AND  (ssu.user_objects_alloc_page_count * 8)  IS NOT NULL; "

$SqlCmd.Connection = $SqlConnection

$SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter

$SqlAdapter.SelectCommand = $SqlCmd

$dataset = New-Object System.Data.DataTable

$SqlAdapter.Fill($dataset)

$SqlConnection.Close()


###Convert the data to JSon directly and select the specific objects needed from the above query, all objects are selected in this case, but you can omit any if needed###

$SynapsePOC=$dataset | Select-Object request_id, loginName, session_id, submit_time,   start_time, end_time,  command,  Space_Allocated_For_User_Objects_KB, Space_Deallocated_For_User_Objects_KB, Space_Allocated_For_Internal_Objects_KB, Space_Deallocated_For_Internal_Objects_KB, MemoryUsage_in_KB, SessionType, RowCount  |ConvertTo-Json






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
