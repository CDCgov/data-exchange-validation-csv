package gov.cdc.dex.csv.services

import com.azure.storage.blob.BlobClient
import com.azure.storage.blob.BlobContainerClient
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.BlobServiceClientBuilder
import java.io.InputStream
import java.io.OutputStream

class AzureBlobServiceImpl(connectionStr:String) :BlobService {
    private val blobServiceClient: BlobServiceClient = BlobServiceClientBuilder().connectionString(connectionStr).buildClient();
    
    override fun doesBlobExist(containerName:String, path: String): Boolean {
        val client = blobServiceClient.getBlobContainerClient(containerName);
        return client.getBlobClient(path).exists()
    }
     
    override fun moveBlob(fromContainerName:String, fromPath: String, toContainerName:String, toPath: String) { 
        val fromClient = blobServiceClient.getBlobContainerClient(fromContainerName);
        val toClient = blobServiceClient.getBlobContainerClient(toContainerName);

        val fromBlob = fromClient.getBlobClient(fromPath);
        val sourceUrl = fromBlob.getBlobUrl();
        toClient.getBlobClient(toPath).blockBlobClient.uploadFromUrl(sourceUrl);
        
        fromBlob.delete()
    }

    override fun getBlobDownloadStream(containerName:String, path: String): InputStream {
        val client = blobServiceClient.getBlobContainerClient(containerName);
        return client.getBlobClient(path).openInputStream();
     }


     override fun getBlobUploadStream(containerName:String, path: String): OutputStream {
        val client = blobServiceClient.getBlobContainerClient(containerName);
        return client.getBlobClient(path).blockBlobClient.getBlobOutputStream();
     }
     
}