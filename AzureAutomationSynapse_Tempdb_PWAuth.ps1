try {




    $dwdb1=Get-AutomationVariable -Name 'Log_Analytics_test_synapse_database_name'
    $SQLDW=Get-AutomationVariable -Name 'Log_Analytics_test_synapse_instance_name'
    $workspaceidsynapse1=Get-AutomationVariable -Name 'Log_Analytics_test_synapse_workspaceidsynapse'
    $workspacekeysynapse=Get-AutomationVariable -Name 'Log_Analytics_test_synapse_workspacekeysynapse'
	$username=Get-AutomationVariable -Name 'SynapseUsername'
	$password=Get-AutomationVariable -Name 'SynpaseSQLPW'



###Context no longer needed as we will get the Synapse SQL Pool instance name from the config parameter.###

### Set-AzContext -SubscriptionId $env:azpocsub

#$SQLDW=@($env:AzureSynapse1);


##You can remove the below in Prod if you like after testing#####

Write-Host $SQLDW



##Write-Host $env:azpocsub

################################################

 

###You can use a foreach loop if there are multiple SQL DWs that require querying, you will have to set the instance and DB for every foreach call###

###The below is using managed identity of the Azure Function, ensure correct permissions is provided to the function in the GRANT VIEW DATABASE STATE TO [automationnamehere]###

###Calls to synapse DW should not incur any concurrency slots of resource usage when querying  DMVs###


$SqlConnection = New-Object System.Data.SqlClient.SqlConnection

$SqlConnection.ConnectionString = "Server=tcp:$SQLDW,1433;Persist Security Info=False;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Initial Catalog=$dwdb1;user=$username;password=$password;"

$SqlCmd = New-Object System.Data.SqlClient.SqlCommand

$SqlCmd.CommandText = "SELECT `
sum(dms.bytes_processed) as 'bytes_written' `
,CAST(sum(dms.bytes_processed)/1024.0/1024.0/1024.0 AS Decimal(10,1)) AS 'gb_written' `
,sum(dms.rows_processed) as 'rows_written' `
,dms.request_id `
,dms.pdw_node_id `
from Sys.dm_pdw_dms_workers dms `
WHERE dms.end_time is not null `
AND dms.type = 'Writer' `
AND dms.destination_info like  '_tempdb%' or dms.destination_info IS NULL `
group by dms.request_id, dms.pdw_node_id `
HAVING CAST(sum(dms.bytes_processed)/1024.0/1024.0/1024.0 AS Decimal(10,1)) > 1"

$SqlCmd.Connection = $SqlConnection
##Added 4min query timeout for larger environments 
$SqlCmd.CommandTimeout=240
$SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
$SqlAdapter.SelectCommand = $SqlCmd
$dataset = New-Object System.Data.DataTable
$SqlAdapter.Fill($dataset)
$SqlConnection.Close()
$SynapseTempDBUsage=($DataSet.Item).count



 


if ($SynapseTempDBUsage -ge 1)
{
# Replace with your Workspace ID From Log Analytics
$CustomerId = $workspaceidsynapse1
# Replace with your Primary Key From Log Analytics
$SharedKey = $workspacekeysynapse
# Specify the name of the record type that you'll be creating For This case it is Synapse Session info which will create a SynapseTempDBUsageDW table in the workspace to query
$LogType = "SynapseTempDBUsageDW"


# You can use an optional field to specify the timestamp from the data. If the time field is not specified, Azure Monitor assumes the time is the message ingestion time

$TimeStampField = ""

###Convert the data to JSon directly and select the specific objects needed from the above query, all objects are selected in this case, but you can omit any if needed###

$SynapsePOC=$dataset | Select-Object request_id, bytes_written, gb_written, rows_written, pdw_node_id   | ConvertTo-Json 






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

###########Send Output of the exception###########

Write-Error -Exception $Exception


} finally {

###########Close any potential open connection###########

if ($SqlConnection.State -eq 'Open') {

$SqlConnection.Close()

}

}
