package gov.cdc.dex.csv.services

import com.azure.storage.blob.BlobClient

interface BlobService{
    fun copyBlobToWorking(path:String, workingParent:String)
}