 

try {


##This code stack leverages Password authentication please ensure passwords are stored in secure location or encrypted.

    $dwdb1=Get-AutomationVariable -Name 'dwdb1'
    $SQLDW=Get-AutomationVariable -Name 'AzureSynapse1'
    $workspaceidsynapse1=Get-AutomationVariable -Name 'workspacekeysynapse'
    $workspacekeysynapse=Get-AutomationVariable -Name 'workspacekeysynapseencrypt'
	$username=Get-AutomationVariable -Name 'SynapseUsername'
	$password=Get-AutomationVariable -Name 'SynpaseSQLPW'

##You can remove the below in Prod if you like after testing#####

Write-Host $SQLDW



##Write-Host $env:azpocsub

################################################

 

###You can use a foreach loop if there are multiple SQL DWs that require querying, you will have to set the instance and DB for every foreach call###

###The below is using SQL Auth of the Azure Automation, ensure correct permissions is provided to the function in the GRANT VIEW DATABASE STATE TO [automationnamehere]###

###Calls to synapse DW should not incur any concurrency slots of resource usage when querying  DMVs###

 


 



$SqlConnection = New-Object System.Data.SqlClient.SqlConnection

$SqlConnection.ConnectionString = "Server=tcp:$SQLDW,1433;Persist Security Info=False;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Initial Catalog=$dwdb1;user=$username;password=$password;"

$SqlCmd = New-Object System.Data.SqlClient.SqlCommand

$SqlCmd.CommandText = "SELECT Count(1) AS TOTAL`
FROM sys.dm_pdw_waits waits  `
JOIN sys.dm_pdw_exec_requests requests  `
ON waits.request_id=requests.request_id  `
WHERE waits.state <> 'Granted' ; "

$SqlCmd.Connection = $SqlConnection

$SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter

$SqlAdapter.SelectCommand = $SqlCmd

$dataset = New-Object System.Data.DataSet

$SqlAdapter.Fill($dataset)

$SqlConnection.Close()

$SynapseWaits=($DataSet.Tables[0]).TOTAL


 


if ($SynapseWaits -ge 1)

{

# Replace with your Workspace ID From Log Analytics

$CustomerId = $workspaceidsynapse1

 

# Replace with your Primary Key From Log Analytics

$SharedKey = $workspacekeysynapse

 

# Specify the name of the record type that you'll be creating For This case it is Synapse Session info which will create a SynapseWaitsDW table in the workspace to query

$LogType = "SynapseWaitsDW"


# You can use an optional field to specify the timestamp from the data. If the time field is not specified, Azure Monitor assumes the time is the message ingestion time

$TimeStampField = ""



# The below metadata will be added to the workspace if the condition is met. There is an initial check above before this section executes to not waste resources


$SqlConnection = New-Object System.Data.SqlClient.SqlConnection

$SqlConnection.ConnectionString = "Server=tcp:$SQLDW,1433;Persist Security Info=False;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Initial Catalog=$dwdb1;user=$username;password=$password;"

$SqlCmd = New-Object System.Data.SqlClient.SqlCommand

$SqlCmd.CommandText = "SELECT waits.session_id, pwsess.Login_Name, pwsess.Login_Time, pwsess.status, pwsess.query_count, `
waits.request_id,  `
requests.command,   `
requests.status,  `
requests.start_time,  `
waits.request_time,  `
waits.acquire_time, `
waits.type,  `
waits.state,  `
waits.object_type,  `
waits.object_name  `
FROM sys.dm_pdw_waits waits  `
JOIN sys.dm_pdw_exec_requests requests  `
ON waits.request_id=requests.request_id `
JOIN  sys.dm_pdw_exec_sessions pwsess `
ON  pwsess.session_id=requests.session_id `
WHERE waits.state <> 'Granted'  `
ORDER BY waits.object_name, waits.object_type, waits.state;"

$SqlCmd.Connection = $SqlConnection

$SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter

$SqlAdapter.SelectCommand = $SqlCmd

$dataset = New-Object System.Data.DataTable

$SqlAdapter.Fill($dataset)

$SqlConnection.Close()


###Convert the data to JSon directly and select the specific objects needed from the above query, all objects are selected in this case, but you can omit any if needed###

$SynapsePOC=$dataset | Select-Object session_id, Login_Name, request_time, request_id, acquire_time, Login_Time,  command, status, start_time, type, state, object_type, object_name    |ConvertTo-Json






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
