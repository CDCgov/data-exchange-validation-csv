package gov.cdc.dex.csv.functions

import com.microsoft.azure.functions.ExecutionContext
import java.util.logging.Logger;
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.io.File
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock;

import gov.cdc.dex.csv.services.BlobService

internal class Unit_FnDecompressor {
    companion object{
        private val outputParentDir = File("target/test-output/"+LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSSSS")))
    }

    private val mockBlobService : BlobService = Mockito.mock(BlobService::class.java);
    private val mockContext : ExecutionContext = Mockito.mock(ExecutionContext::class.java);
    private val logger : Logger = Mockito.mock(Logger::class.java);

    private val testFnDebatcher : FnDecompressor = FnDecompressor(mockBlobService);

    @BeforeEach
    fun initiateMocks(){
        Mockito.`when`(mockContext.logger).thenReturn(logger)
        Mockito.`when`(logger.info(Mockito.anyString())).thenAnswer(this::loggerInvocation)
        
        Mockito.`when`(mockBlobService.doesIngestBlobExist(Mockito.anyString())).thenAnswer(this::doesTestFileExist)
        Mockito.`when`(mockBlobService.copyIngestBlobToProcess(Mockito.anyString(),Mockito.anyString())).thenAnswer(this::copyTestFile)
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
        var map = defaultMap(url="DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/testfiles/test-upload.zip", contentType="text/zip",id="happy_zip")
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
        var map = defaultMap(url="DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/testfiles/test-upload.csv", contentType="text/zip",id="bad_zip")
        var message = buildTestMessage(map);
        
        testFnDebatcher.process(message,mockContext);
        //TODO verify the event was triggered
        //TODO verify the file is in the directory
    }

    @Test
    internal fun negative_file_emptyZip(){
        var map = defaultMap(url="DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/testfiles/test-empty.zip", contentType="text/zip",id="empty_zip")
        var message = buildTestMessage(map);
        
        testFnDebatcher.process(message,mockContext);
        //TODO verify the event was triggered
        //TODO verify the file is in the directory
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //internal Azure error, unsure how to handle or how to even test....

    @Test
    internal fun negative_integration_unableToConnectToStorage(){
        //TODO
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //helper functions

    private fun loggerInvocation(i: InvocationOnMock){
        val toLog:String = i.getArgument(0);
        println(toLog);
        //TODO maybe do something with this in the tests
    }
    
    private fun doesTestFileExist(i: InvocationOnMock):Boolean{
        val relativePath:String = i.getArgument(0);

        var localFile = File("src/test/resources",relativePath);
        return localFile.exists();
    }
    
    private fun copyTestFile(i: InvocationOnMock):String{
        val relativePathIn:String = i.getArgument(0);
        val outPathPrefix:String = i.getArgument(1);

        val relativePathOut = outPathPrefix+relativePathIn;

        var localFileIn = File("src/test/resources",relativePathIn);
        var localFileOut = File(outputParentDir,relativePathOut);

        localFileOut.parentFile.mkdirs()

        localFileIn.copyTo(localFileOut);

        return relativePathOut;
    }

    private fun defaultMap(id:String = "DUMMY_ID",url:String = "DUMMY_URL", contentType:String = "DUMMY_CONTENT_TYPE"): MutableMap<*,*>{
        val mapData = mutableMapOf("url" to url, "contentType" to contentType, "extraField" to "DUMMY_EXTRA_FIELD");
        return mutableMapOf("eventType" to "Microsoft.Storage.BlobCreated", "id" to id, "data" to mapData, "extraField" to "DUMMY_EXTRA_FIELD");
    }

    private fun buildTestMessage(mapObj:Map<*,*>): String { 
        val message = buildMessageFromMap(mapObj);
        return "[[$message]]"
    }

    private fun buildMessageFromMap(map:Map<*,*>):String{
        return map.map { (k,v)-> 
            var value = if(v is Map<*,*>){buildMessageFromMap(v)}else{"\"$v\""};
            var combined="\"$k\":$value";
            combined
        }
        .joinToString(separator=",",prefix="{",postfix="}") 
    }
}