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

class GenericCSVValidationFileFun {
    @FunctionName("GenericCSVValidationFileFun")
    fun GenericCSVValidationFileFun(
        @HttpTrigger(
            name = "req",
            methods = [HttpMethod.GET, HttpMethod.POST],
            authLevel = AuthorizationLevel.FUNCTION) request: HttpRequestMessage<Optional<String>>,
            context: ExecutionContext): HttpResponseMessage { 
                
        context.logger.info("HTTP trigger processed a ${request.httpMethod.name} request.")

        val connectionString = System.getenv("BlobConnection")
        val containerName = request.queryParameters["containername"]
        val filePath: String? = request.queryParameters["filepath"]


        val backupStorageAccount = CloudStorageAccount.parse(connectionString)
        var backupBlobClient = backupStorageAccount.createCloudBlobClient()
        var container = backupBlobClient.getContainerReference(containerName)
        container.createIfNotExists()
        val blobs = container.listBlobs(null, true)

        val responseBody = StringBuilder("")

        for (blobItem in blobs) {
            if (blobItem is CloudBlob) {
                val blob = blobItem.name
                if (blob == filePath){
                    if (blob.endsWith(".csv")){
                        val file = File(blob)
                        val filename = file.name
                        responseBody.append("Validation Passed for $filename. $filename is csv file")
                    }
                    else{
                        val file = File(blob)
                        val filename = file.name
                        responseBody.append("Validation Failed for $filename. $filename is not a csv file")
                    }
                }
            }
            else {
                continue
            }
        }

        val responseMessage = request.createResponseBuilder(HttpStatus.OK)
        .header("Content-Type", "text/plain")
        .body(responseBody)
        .build()

        return responseMessage

    }
}
