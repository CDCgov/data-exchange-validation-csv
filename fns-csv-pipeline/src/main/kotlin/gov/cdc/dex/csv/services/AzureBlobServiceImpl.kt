package gov.cdc.dex.csv.services

import com.azure.storage.blob.BlobClient
import com.azure.storage.blob.BlobContainerClient
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.BlobServiceClientBuilder

class AzureBlobServiceImpl(connectionStr: String) :BlobService {
    private val ingestBlobContainer = System.getenv("BlobContainer_Ingest")
    private val workingBlobContainer = System.getenv("BlobContainer_Working")
    private val errorBlobContainer = System.getenv("BlobContainer_Error")

    private val blobServiceClient: BlobServiceClient = BlobServiceClientBuilder().connectionString(connectionStr).buildClient();
    private val ingestClient: BlobContainerClient = blobServiceClient.getBlobContainerClient(ingestBlobContainer);
    private val workingClient: BlobContainerClient = blobServiceClient.getBlobContainerClient(workingBlobContainer);
    private val errorClient: BlobContainerClient = blobServiceClient.getBlobContainerClient(errorBlobContainer);

    override fun copyBlobToWorking(path: String, workingParent:String) { 
        var sourceUrl = ingestClient.getBlobClient(path).getBlobUrl();
        workingClient.getBlobClient(workingParent+"/"+path).blockBlobClient.uploadFromUrl(sourceUrl)
    }

}