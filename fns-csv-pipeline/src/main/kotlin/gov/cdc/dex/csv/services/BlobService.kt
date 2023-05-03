package gov.cdc.dex.csv.services

import com.azure.storage.blob.BlobClient
import java.io.InputStream
import java.io.OutputStream

interface BlobService{
    fun doesIngestBlobExist(path:String):Boolean
    fun copyIngestBlobToProcess(path:String, processParent:String):String
    fun getProcessBlobInputStream(path:String):InputStream
    fun getProcessBlobOutputStream(path:String):OutputStream
}