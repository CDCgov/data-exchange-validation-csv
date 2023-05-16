package gov.cdc.dex.csv.services

import com.azure.storage.blob.BlobClient
import com.azure.storage.blob.BlobContainerClient
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.BlobServiceClientBuilder
import java.io.InputStream
import java.io.OutputStream
import java.io.BufferedOutputStream
import java.io.BufferedInputStream

class AzureBlobServiceImpl(connectionStr:String) :BlobService {
    private val BUFFER_SIZE = 4096
    private val blobServiceClient: BlobServiceClient = BlobServiceClientBuilder().connectionString(connectionStr).buildClient();
    
    override fun doesBlobExist(containerName:String, path: String): Boolean {
        val client = blobServiceClient.getBlobContainerClient(containerName);
        return client.getBlobClient(path).exists()
    }
    
    override fun getBlobMetadata(containerName:String, path:String):Map<String,String>{
        val client = blobServiceClient.getBlobContainerClient(containerName);
        return client.getBlobClient(path).properties.metadata.mapKeys{it.key.lowercase()}
    }
     
    override fun moveBlob(fromContainerName:String, fromPath: String, toContainerName:String, toPath: String):String { 
        val fromClient = blobServiceClient.getBlobContainerClient(fromContainerName);
        val toClient = blobServiceClient.getBlobContainerClient(toContainerName);

        val fromBlob = fromClient.getBlobClient(fromPath);
        val toBlob = toClient.getBlobClient(toPath)

        //TODO there are other Azure functions for copying file, such as fromUrl
        //however, I had problems getting them to work
        //it might be worth revisiting down the line
        BufferedInputStream(fromBlob.openInputStream()).use{ fromStream ->
            BufferedOutputStream(toBlob.blockBlobClient.getBlobOutputStream()).use{ toStream ->
                val bytesIn = ByteArray(BUFFER_SIZE)
                var read: Int
                while (fromStream.read(bytesIn).also { read = it } != -1) {
                    toStream.write(bytesIn, 0, read)
                }
            }
        }
        
        fromBlob.delete()
        return toBlob.getBlobUrl()
    }

    override fun getBlobDownloadStream(containerName:String, path: String): InputStream {
        val client = blobServiceClient.getBlobContainerClient(containerName);
        return client.getBlobClient(path).openInputStream();
     }


     override fun getBlobUploadStream(containerName:String, path: String): Pair<OutputStream,String> {
        val client = blobServiceClient.getBlobContainerClient(containerName);
        val blob = client.getBlobClient(path);
        return Pair(blob.blockBlobClient.getBlobOutputStream(),blob.getBlobUrl());
     }
     
}