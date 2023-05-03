package gov.cdc.dex.csv.functions

import com.microsoft.azure.functions.ExecutionContext

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.logging.Logger;

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock;

import gov.cdc.dex.csv.services.BlobService
import gov.cdc.dex.csv.test.utils.*

internal class Unit_FnDecompressor {
    companion object{
        private val outputParentDir = File("target/test-output/"+LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSSSS")))
        private val ingestDir = File("src/test/resources")
        private val processDir = File(outputParentDir,"process")
        private val errorDir = File(outputParentDir,"error")

    }

    private val mockContext = mockContext();
    private val mockBlobService : BlobService = Mockito.mock(BlobService::class.java);
    private val testFnDebatcher : FnDecompressor = FnDecompressor(mockBlobService);
    @BeforeEach
    fun initiateMocks(){
        processDir.mkdirs()
        errorDir.mkdirs()

        Mockito.`when`(mockBlobService.doesIngestBlobExist(Mockito.anyString())).thenAnswer(this::doesTestFileExist)
        Mockito.`when`(mockBlobService.copyIngestBlobToProcess(Mockito.anyString(),Mockito.anyString())).thenAnswer(this::copyTestFile)
        Mockito.`when`(mockBlobService.getProcessBlobInputStream(Mockito.anyString())).thenAnswer(this::openInputStream)
        Mockito.`when`(mockBlobService.getProcessBlobOutputStream(Mockito.anyString())).thenAnswer(this::openOutputStream)
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        //happy path
    

    @Test
    internal fun happyPath_singleCsv(){
        var map = defaultMap(url="DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/testfiles/test-upload.csv", contentType="text/csv",id="happy_single")
        var message = buildTestMessage(map);
        
        testFnDebatcher.process(message,mockContext);
        //TODO verify the event was triggered
        //TODO verify the file is in the directory
    }

    @Test
    internal fun happyPath_zip(){
        var map = defaultMap(url="DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/testfiles/test-upload.zip", contentType="application/zip",id="happy_zip")
        var message = buildTestMessage(map);
        
        testFnDebatcher.process(message,mockContext);
        //TODO verify the events were triggered
        //TODO verify the files are in the directory
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //bad messages, throw exceptions

    @Test
    internal fun negative_message_empty(){
        val e = Assertions.assertThrows(IllegalArgumentException::class.java) {
            testFnDebatcher.process("",mockContext);
        }

        Assertions.assertEquals("Empty message from Azure!", e.message)
    }

    @Test
    internal fun negative_message_badFormat(){
        Assertions.assertThrows(com.google.gson.JsonSyntaxException::class.java) {
            testFnDebatcher.process("][",mockContext);
        }
    }

    @Test
    internal fun negative_message_missingEventType(){
        var map = defaultMap(id="no_event_type");
        map.remove("eventType");
        var message = buildTestMessage(map);

        testFnDebatcher.process(message,mockContext);
        //TODO make sure that nothing happens
    }

    @Test
    internal fun negative_message_missingId(){
        var map = defaultMap();
        map.remove("id");
        var message = buildTestMessage(map);

        val e = Assertions.assertThrows(IllegalArgumentException::class.java) {
            testFnDebatcher.process(message,mockContext);
        }
        
        Assertions.assertEquals("Azure message missing id!", e.message)
    }

    @Test
    internal fun negative_message_missingData(){
        var map = defaultMap();
        map.remove("data");
        var message = buildTestMessage(map);

        val e = Assertions.assertThrows(IllegalArgumentException::class.java) {
            testFnDebatcher.process(message,mockContext);
        }
        
        Assertions.assertEquals("Azure message missing blob content-type!", e.message)
    }

    @Test
    internal fun negative_message_missingContentType(){
        var map = defaultMap();
        var dataMap = map["data"] as MutableMap<*,*>;
        dataMap.remove("contentType");
        var message = buildTestMessage(map);

        val e = Assertions.assertThrows(IllegalArgumentException::class.java) {
            testFnDebatcher.process(message,mockContext);
        }
        
        Assertions.assertEquals("Azure message missing blob content-type!", e.message)
    }

    @Test
    internal fun negative_message_missingUrl(){
        var map = defaultMap();
        var dataMap = map["data"] as MutableMap<*,*>;
        dataMap.remove("url");
        var message = buildTestMessage(map);

        val e = Assertions.assertThrows(IllegalArgumentException::class.java) {
            testFnDebatcher.process(message,mockContext);
        }
        
        Assertions.assertEquals("Azure message missing blob URL!", e.message)
    }

    @Test
    internal fun negative_message_badUrl(){
        var map = defaultMap();
        var message = buildTestMessage(map);

        val e = Assertions.assertThrows(IllegalArgumentException::class.java) {
            testFnDebatcher.process(message,mockContext);
        }
        
        Assertions.assertEquals("Azure message had bad URL for the file! DUMMY_URL", e.message)
    }

    @Test
    internal fun negative_file_missingFile(){
        var map = defaultMap(url="DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/test-not-there")
        var message = buildTestMessage(map);
        
        val e = Assertions.assertThrows(IllegalArgumentException::class.java) {
            testFnDebatcher.process(message,mockContext);
        }
        
        Assertions.assertEquals("File missing in Azure! DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/test-not-there", e.message)
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //bad file, create fail message

    @Test
    internal fun negative_file_unableToZip(){
        var map = defaultMap(url="DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/testfiles/test-upload.csv", contentType="application/zip",id="bad_zip")
        var message = buildTestMessage(map);
        
        testFnDebatcher.process(message,mockContext);
        //TODO verify the event was triggered
        //TODO verify the file is in the directory
    }

    @Test
    internal fun negative_file_emptyZip(){
        var map = defaultMap(url="DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/testfiles/test-empty.zip", contentType="application/zip",id="empty_zip")
        var message = buildTestMessage(map);
        
        testFnDebatcher.process(message,mockContext);
        //TODO verify the event was triggered
        //TODO verify the file is in the directory
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //helper functions

    private fun doesTestFileExist(i: InvocationOnMock):Boolean{
        val relativePath:String = i.getArgument(0);

        var localFile = File(ingestDir,relativePath);
        return localFile.exists();
    }
    
    private fun copyTestFile(i: InvocationOnMock):String{
        val relativePathIn:String = i.getArgument(0);
        val outPathPrefix:String = i.getArgument(1);

        val relativePathOut = outPathPrefix+relativePathIn;

        var localFileIn = File(ingestDir,relativePathIn);
        var localFileOut = File(processDir,relativePathOut);

        localFileOut.parentFile.mkdirs()

        localFileIn.copyTo(localFileOut);

        return relativePathOut;
    }
    
    private fun openInputStream(i: InvocationOnMock):InputStream{
        val relativePath:String = i.getArgument(0);

        var localFile = File(processDir,relativePath);
        return FileInputStream(localFile);
    }
    
    private fun openOutputStream(i: InvocationOnMock):OutputStream{
        val relativePath:String = i.getArgument(0);

        var localFile = File(processDir,relativePath);
        localFile.parentFile.mkdirs();
        return FileOutputStream(localFile);
    }
}