package gov.cdc.dex.csv.functions

import gov.cdc.dex.csv.services.AzureBlobServiceImpl
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions

internal class Integration_AzureBlobServiceImpl {
    //TO RUN THIS TEST
    // - grab the parameters from Azure
    // - upload test file "test-upload.csv" to the processed container
    companion object{
        private val connectionStr = ""
        private val baseUrl = ""
        private val processedContainer = ""

        private val service = AzureBlobServiceImpl(connectionStr)
    }
    
    @Test
    internal fun happyPath_doesBlobExist(){
        val bool = service.doesBlobExist(processedContainer, "test-upload.csv")
        Assertions.assertTrue(bool, "could not see file in processed!")
    }
    
    @Test
    internal fun happyPath_moveBlob(){
        val url = service.moveBlob(processedContainer, "test-upload.csv",processedContainer, "moved/test-upload.csv")
        Assertions.assertEquals(baseUrl+processedContainer+"/moved%2Ftest-upload.csv",url)
        val bool = service.doesBlobExist(processedContainer, "moved/test-upload.csv")
        Assertions.assertTrue(bool, "could not see file in processed!")
        
        val urlBack = service.moveBlob(processedContainer, "moved/test-upload.csv",processedContainer, "test-upload.csv")
        Assertions.assertEquals(baseUrl+processedContainer+"/test-upload.csv",urlBack)
        val boolBack = service.doesBlobExist(processedContainer, "test-upload.csv")
        Assertions.assertTrue(boolBack, "could not see file in processed!")
    }
}