 

try {




    $dwdb1=Get-AutomationVariable -Name 'Log_Analytics_test_synapse_database_name'
    $SQLDW=Get-AutomationVariable -Name 'Log_Analytics_test_synapse_instance_name'
    $workspaceidsynapse1=Get-AutomationVariable -Name 'Log_Analytics_test_synapse_workspaceidsynapse'
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

###Calls to synapse DW should not incur any concurrency slots of resource usage when quiring DMVs###

 


 


$resourceURI = "https://database.windows.net/"

$tokenAuthURI = $env:MSI_ENDPOINT + "?resource=$resourceURI&api-version=2017-09-01"

$tokenResponse = Invoke-RestMethod -Method Get -Headers @{"Secret"="$env:MSI_SECRET"} -Uri $tokenAuthURI

$accessToken = $tokenResponse.access_token

$SqlConnection = New-Object System.Data.SqlClient.SqlConnection

$SqlConnection.ConnectionString = "Server=tcp:$SQLDW,1433;Persist Security Info=False;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Initial Catalog=$dwdb1;"

$SqlConnection.AccessToken = $AccessToken

$SqlCmd = New-Object System.Data.SqlClient.SqlCommand

$SqlCmd.CommandText = "SELECT Count(1) AS TOTAL from sys.dm_pdw_nodes_exec_query_memory_grants  nomemgran left join sys.dm_pdw_nodes_exec_sql_text noneexecsqltxt `
on noneexecsqltxt.sql_handle=nomemgran.sql_handle `
and noneexecsqltxt.session_id=nomemgran.session_id `
left join sys.dm_pdw_exec_sessions pwsess `
on pwsess.request_id like   SUBSTRING(noneexecsqltxt.text, 41,PATINDEX('%'',%', noneexecsqltxt.text )-41) --verify in large requst IDs this return results `
where nomemgran.request_time >=  DATEADD(minute,-5,getdate()) `
AND pwsess.session_id<> session_id(); "

$SqlCmd.Connection = $SqlConnection

$SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter

$SqlAdapter.SelectCommand = $SqlCmd

$dataset = New-Object System.Data.DataSet

$SqlAdapter.Fill($dataset)

$SqlConnection.Close()

$SynapseMemory=($DataSet.Tables[0]).TOTAL


 


if ($SynapseMemory -ge 1)

{

# Replace with your Workspace ID From Log Analytics

$CustomerId = $workspaceidsynapse1

 

# Replace with your Primary Key From Log Analytics

$SharedKey = $workspacekeysynapse

 

# Specify the name of the record type that you'll be creating For This case it is Synapse Session info which will create a SynapseMemoryDW table in the workspace to query

$LogType = "SynapseMemoryDW"


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

$SqlCmd.CommandText = "select  nomemgran.session_id, nomemgran.request_id, nomemgran.dop, nomemgran.request_time, nomemgran.grant_time, `
nomemgran.requested_memory_kb,  required_memory_kb, nomemgran.used_memory_kb, nomemgran.max_used_memory_kb, nomemgran.query_cost, `
nomemgran.sql_handle,  noneexecsqltxt.text, pwsess.login_name `
from sys.dm_pdw_nodes_exec_query_memory_grants  nomemgran left join sys.dm_pdw_nodes_exec_sql_text noneexecsqltxt `
on noneexecsqltxt.sql_handle=nomemgran.sql_handle `
and noneexecsqltxt.session_id=nomemgran.session_id `
left join sys.dm_pdw_exec_sessions pwsess `
on pwsess.request_id like   SUBSTRING(noneexecsqltxt.text, 41,PATINDEX('%'',%', noneexecsqltxt.text )-41) --verify in large requst IDs this return results `
where nomemgran.request_time >=  DATEADD(minute,-5,getdate()) `
AND pwsess.session_id<> session_id();"

$SqlCmd.Connection = $SqlConnection

$SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter

$SqlAdapter.SelectCommand = $SqlCmd

$dataset = New-Object System.Data.DataTable

$SqlAdapter.Fill($dataset)

$SqlConnection.Close()


###Convert the data to JSon directly and select the specific objects needed from the above query, all objects are selected in this case, but you can omit any if needed###

$SynapsePOC=$dataset | Select-Object session_id, dop, request_time,  grant_time, requested_memory_kb, granted_memory_kb,  required_memory_kb,   used_memory_kb, max_used_memory_kb, ideal_memory_kb,  query_cost, wait_time_ms, plan_handle,  sql_handle, request_id, step_index,pdw_node_id, distribution_id, status, start_time, end_time, text, login_name   |ConvertTo-Json






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
