
try {
 
 
    ##This code stack leverages Password authentication please ensure passwords are stored in secure location or encrypted.
   ##Set all variables to call specific services and authentication.
     
    
        $dwdb1= $env:SQLPool
    
        $SQLDW=$env:AzureSynapseDW
        
        $username= $env:suername
    
        $password= $env:password
        $blobName = "synpaseerrors_"+(Get-Date).tostring("MM_dd_yyyy_hh_mm_ss")+".json"
        $blobservicesastoken = $env:saskeyfromazurestorage
        $blobURL=$env:bloburlpathtostorefile

    ##You can remove the below in Prod if you like after testing#####
    
     
    
    Write-Host $SQLDW
    
     
    
     
    
     
    
    ################################################
       
    ###You can use a foreach loop if there are multiple SQL DWs that require querying, you will have to set the instance and DB for every foreach call###
    ###The below is using SQL Auth of the Azure Automation, ensure correct permissions is provided to the function in the GRANT VIEW DATABASE STATE TO [automationnamehere]###
     
    
    ###Calls to synapse DW should not incur any concurrency slots of resource usage when querying  DMVs###
    
    $SqlConnection = New-Object System.Data.SqlClient.SqlConnection
    $SqlConnection.ConnectionString = "Server=tcp:$SQLDW,1433;Persist Security Info=False;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=True;Initial Catalog=$dwdb1;user=$username;password=$password;"
    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand
    $SqlCmd.CommandText = "select * From sys.dm_pdw_errors where `
    create_time >= DATEADD(minute,-5,getdate())"
    $SqlCmd.Connection = $SqlConnection
    $SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
    $SqlAdapter.SelectCommand = $SqlCmd
    $dataset = New-Object System.Data.DataTable
    $SqlAdapter.Fill($dataset)
    $SqlConnection.Close()
    $SynapseErrors=($DataSet.Item).count

    if ($SynapseErrors -ge 1)
    {
    # The below metadata will be added to the db if the condition is met. There is an initial check above before this section executes to not waste resources
    $SynapsePOC=$dataset | Select-Object error_id, source, type, create_time, pdw_node_id, session_id, request_id,  spid, thread_id, details   | ConvertTo-Json 
    Write-Output $SynapsePOC
# Create a SAS URL
$sasUrl = $blobURL
    
    
# Set request headers
$headers = @{"x-ms-blob-type"="BlockBlob"}
    
      
     
$body = $SynapsePOC
#Invoke "Put Blob" REST API
Invoke-RestMethod -Method "PUT" -Uri $sasUrl -Body $body -Headers $headers -ContentType "text/json"
     
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
