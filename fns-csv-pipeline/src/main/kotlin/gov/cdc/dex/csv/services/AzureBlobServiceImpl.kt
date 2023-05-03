package gov.cdc.dex.csv.services

import com.azure.storage.blob.BlobClient
import com.azure.storage.blob.BlobContainerClient
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.BlobServiceClientBuilder
import java.io.InputStream
import java.io.OutputStream

class AzureBlobServiceImpl(connectionStr:String, ingestBlobContainer:String, processedBlobContainer:String, errorBlobContainer:String) :BlobService {
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


     override fun getProcessBlobOutputStream(path: String): OutputStream {
        return processedClient.getBlobClient(path).blockBlobClient.getBlobOutputStream();
     }
     
}