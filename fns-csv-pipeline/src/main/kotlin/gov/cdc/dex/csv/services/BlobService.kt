package gov.cdc.dex.csv.services

import com.azure.storage.blob.BlobClient
import java.io.InputStream

interface BlobService{
    fun doesIngestBlobExist(path:String):Boolean
    fun copyIngestBlobToProcess(path:String, processParent:String):String
    fun getProcessBlobInputStream(path:String):InputStream
}