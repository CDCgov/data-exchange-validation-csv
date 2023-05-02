package gov.cdc.dex.csv.services

import com.azure.storage.blob.BlobClient
import com.azure.storage.blob.BlobContainerClient
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.BlobServiceClientBuilder
import java.io.InputStream

class AzureBlobServiceImpl(connectionStr: String) :BlobService {
    private val ingestBlobContainer = System.getenv("BlobContainer_Ingest")
    private val processedBlobContainer = System.getenv("BlobContainer_Processed")
    private val errorBlobContainer = System.getenv("BlobContainer_Error")

    private val blobServiceClient: BlobServiceClient = BlobServiceClientBuilder().connectionString(connectionStr).buildClient();
    private val ingestClient: BlobContainerClient = blobServiceClient.getBlobContainerClient(ingestBlobContainer);
    private val processedClient: BlobContainerClient = blobServiceClient.getBlobContainerClient(processedBlobContainer);
    private val errorClient: BlobContainerClient = blobServiceClient.getBlobContainerClient(errorBlobContainer);

    override fun doesIngestBlobExist(path: String): Boolean {
        return ingestClient.getBlobClient(path).exists()
    }
 
    override fun copyIngestBlobToProcess(path: String, processParent:String):String { 
        var newPath = processParent+"/"+path;
        var sourceUrl = ingestClient.getBlobClient(path).getBlobUrl();
        processedClient.getBlobClient(newPath).blockBlobClient.uploadFromUrl(sourceUrl);
        return newPath;
    }

    override fun getProcessBlobInputStream(path: String): InputStream {
        return processedClient.getBlobClient(path).openInputStream();
     }

 
}