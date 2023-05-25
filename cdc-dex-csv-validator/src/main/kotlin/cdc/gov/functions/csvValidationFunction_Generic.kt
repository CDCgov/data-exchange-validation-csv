package cdc.gov.functions

import java.util.*
import java.lang.Object.*
import com.microsoft.azure.functions.*
import com.microsoft.azure.functions.annotation.*
import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob.CloudBlobClient.*
import com.microsoft.azure.storage.blob.CloudBlobContainer.*
import com.microsoft.azure.storage.blob.CloudBlockBlob.*
import com.microsoft.azure.storage.blob.*

import java.util.*
import java.io.File
import java.lang.StringBuilder

//This function receives contianername as an input and provide the Generic validation results as validation passed(if csv file found) and Validation failed section(if non csv files found)
//For both validation passed and validation failed files -> folder name and file names are displayed for futher analysis. 

class CSVGenericValidationFunction {
    @FunctionName("CSVGenericValidationFunction")
    fun runCSVGenericValidationFunction(
        @HttpTrigger(
            name = "req",
            methods = [HttpMethod.GET, HttpMethod.POST],
            authLevel = AuthorizationLevel.FUNCTION) request: HttpRequestMessage<Optional<String>>,
            context: ExecutionContext): HttpResponseMessage { 
                
        context.logger.info("HTTP trigger processed a ${request.httpMethod.name} request.")
        
        // reterive the connection string from application settings of Function App

        val connectionString = System.getenv("BlobConnection")

        //receive the containername from query parameter
        val containerName = request.queryParameters["containername"]

        //if the containername query parameter is not received in the request, display the below message
        if (containerName == null)   {       
        //val responseMessage = 
        return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
        .body("Please pass containername as a query parameter")
        .build()
       // return responseMessage
    }  

        val validationPassed = mutableMapOf<String, String>()
        val validationFailed = mutableMapOf<String, String>()

        //read the cloud blob container for file & foldernames 
        val backupStorageAccount = CloudStorageAccount.parse(connectionString)
        var backupBlobClient = backupStorageAccount.createCloudBlobClient()
        var container = backupBlobClient.getContainerReference(containerName)
        container.createIfNotExists()

        val blobs = container.listBlobs(null, true)

        
        //Iterate through each blob item and determine if its csv file 
        for (blobItem in blobs) {
            if (blobItem is CloudBlob) {
               val folderName = blobItem.getParent().prefix
                val blob = blobItem.name

                if (blob.toLowerCase().endsWith(".csv")) {
                    val file = File(blob)
                    val fileName = file.name                    
                    validationPassed[fileName] = folderName
                } 
                //else if (blob.toLowerCase().endsWith(".*")){
                else if (blob.toLowerCase().endsWith(".xml") || blob.toLowerCase().endsWith(".gz") || blob.toLowerCase().endsWith(".zip") || blob.toLowerCase().endsWith(".txt")|| blob.toLowerCase().endsWith(".parquet") || blob.toLowerCase().endsWith(".xls*")||blob.toLowerCase().endsWith(".xlsx") || blob.toLowerCase().endsWith(".*")){
                    val file = File(blob)
                    val fileName = file.name                    
                    validationFailed[fileName] = folderName
                }
                else{
                    continue
                }

            }
            else{
                continue
            }
        }

        //update the validation passed part of response body based on the above logic
        val responseBody = StringBuilder("")
        responseBody.append("Generic CSV validation results \n")
        responseBody.append("Validation Passed: \n")
        if (validationPassed.isEmpty()){
            responseBody.append("No csv files found in the container: $containerName \n")
        }
        else{
            for ((fileName, folderName) in validationPassed) {
                responseBody.append("Folder Name: $folderName and FileName: $fileName \n")
            }
        }
        
        //update the validation failed part of response body based on the above logic

        responseBody.append("\n Validation Failed: \n")
        if (validationFailed.isEmpty()){
            responseBody.append("No non csv files found in the container: $containerName \n")
            }
        else{
            for ((fileName, folderName) in validationFailed) {
                responseBody.append("Folder Name: $folderName and FileName: $fileName \n")
        }
    }
        
        containerName?.let {
        val responseMessage = request.createResponseBuilder(HttpStatus.OK)
        .header("Content-Type", "text/plain")
        .body(responseBody)
        .build()
        return responseMessage
        }
        
        //val responseMessage = 
        //return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
        //.body("Please pass a containername on the query string")
        //.build()
       // return responseMessage

    }
}
