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
import gov.cdc.dex.csv.services.EventService
import gov.cdc.dex.csv.dtos.ConnectionNames
import gov.cdc.dex.csv.dtos.EventHubNames
import gov.cdc.dex.csv.dtos.BlobStorageNames

internal class Unit_FnDecompressor {
    companion object{
        private val outputParentDir = File("target/test-output/"+LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSSSS")))

        private val ingestDir = File(outputParentDir,"ingest");
        private val processDir = File(outputParentDir,"process");
        private val errorDir = File(outputParentDir,"error");

        private val blobNames = BlobStorageNames(ingestDir.name, processDir.name, errorDir.name)
        private val eventHubNames = EventHubNames("ingest", "ok", "fail");
        private val connectionNames = ConnectionNames(eventHubNames, blobNames);
    }

    private val mockContext = mockContext();
    private val mockBlobService : BlobService = Mockito.mock(BlobService::class.java);
    private val mockEventService : EventService = Mockito.mock(EventService::class.java);
    private val testFnDebatcher : FnDecompressor = FnDecompressor(mockBlobService,mockEventService,connectionNames);

    private val eventMap = mutableMapOf<String,MutableList<String>>()

    @BeforeEach
    fun initiateMocks(){
        Mockito.`when`(mockBlobService.doesBlobExist(Mockito.anyString(),Mockito.anyString())).thenAnswer(this::doesTestFileExist)
        Mockito.`when`(mockBlobService.getBlobMetadata(Mockito.anyString(),Mockito.anyString())).thenAnswer(this::getMetadata)
        Mockito.`when`(mockBlobService.moveBlob(Mockito.anyString(),Mockito.anyString(),Mockito.anyString(),Mockito.anyString())).thenAnswer(this::moveTestFile)
        Mockito.`when`(mockBlobService.getBlobDownloadStream(Mockito.anyString(),Mockito.anyString())).thenAnswer(this::openInputStream)
        Mockito.`when`(mockBlobService.getBlobUploadStream(Mockito.anyString(),Mockito.anyString())).thenAnswer(this::openOutputStream)

        Mockito.`when`(mockEventService.sendOne(Mockito.anyString(),Mockito.anyString())).thenAnswer(this::addEvent)
        Mockito.`when`(mockEventService.sendBatch(Mockito.anyString(),Mockito.anyList())).thenAnswer(this::addEvents)
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        //happy path
    

    @Test
    internal fun happyPath_singleCsv(){
        copyToIngest("test-upload.csv")
        var map = defaultMap(url="DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/test-upload.csv", contentType="text/csv",id="happy_single")
        var message = buildTestMessage(map);
        
        testFnDebatcher.process(message,mockContext);

        Assertions.assertTrue(ingestDir.list().isEmpty(), "ingest file not empty!")
        Assertions.assertTrue(File(processDir,"happy_single/test-upload.csv").exists(), "file not in processed!")
        Assertions.assertFalse(File(errorDir,"happy_single").exists(), "incorrect error dir created!")

        Assertions.assertTrue(eventMap["fail"].isNullOrEmpty(), "incorrect fail message")
        var okMessageList = eventMap["ok"]
        Assertions.assertFalse(okMessageList.isNullOrEmpty(), "missing ok message")
        Assertions.assertEquals(1, okMessageList?.size, "too many ok messages")
    }

    @Test
    internal fun happyPath_zip(){
        copyToIngest("test-upload-zip.zip")
        var map = defaultMap(url="DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/test-upload-zip.zip", contentType="application/zip",id="happy_zip")
        var message = buildTestMessage(map);
        
        testFnDebatcher.process(message,mockContext);

        Assertions.assertTrue(ingestDir.list().isEmpty(), "ingest file not empty!")
        Assertions.assertTrue(File(processDir,"happy_zip/test-upload-zip.zip").exists(), "file not in processed!")
        Assertions.assertTrue(File(processDir,"happy_zip/test-upload-zip/test-upload-1.csv").exists(), "file not in processed!")
        Assertions.assertTrue(File(processDir,"happy_zip/test-upload-zip/test-upload-2.csv").exists(), "file not in processed!")
        Assertions.assertTrue(File(processDir,"happy_zip/test-upload-zip/test-upload-3/test-upload.csv").exists(), "file not in processed!")
        Assertions.assertTrue(File(processDir,"happy_zip/test-upload-zip/test-upload-inner/test-upload-4.csv").exists(), "file not in processed!")
        Assertions.assertFalse(File(errorDir,"happy_zip").exists(), "incorrect error dir created!")

        Assertions.assertTrue(eventMap["fail"].isNullOrEmpty(), "incorrect fail message")
        var okMessageList = eventMap["ok"]
        Assertions.assertFalse(okMessageList.isNullOrEmpty(), "missing ok message")
        Assertions.assertEquals(4, okMessageList?.size, "too many ok messages")
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //bad messages, throw exceptions

    @Test
    internal fun negative_message_empty(){
        val e = Assertions.assertThrows(IllegalArgumentException::class.java) {
            testFnDebatcher.process("",mockContext);
        }

        Assertions.assertEquals("Empty message from Azure!", e.message)
        
        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        Assertions.assertTrue(eventMap["fail"].isNullOrEmpty(), "incorrect fail message")
    }

    @Test
    internal fun negative_message_badFormat(){
        Assertions.assertThrows(com.google.gson.JsonSyntaxException::class.java) {
            testFnDebatcher.process("][",mockContext);
        }
        
        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        Assertions.assertTrue(eventMap["fail"].isNullOrEmpty(), "incorrect fail message")
    }

    @Test
    internal fun negative_message_missingEventType(){
        var map = defaultMap(id="no_event_type");
        map.remove("eventType");
        var message = buildTestMessage(map);

        testFnDebatcher.process(message,mockContext);
        
        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        Assertions.assertTrue(eventMap["fail"].isNullOrEmpty(), "incorrect fail message")
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
        
        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        Assertions.assertTrue(eventMap["fail"].isNullOrEmpty(), "incorrect fail message")
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
        
        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        Assertions.assertTrue(eventMap["fail"].isNullOrEmpty(), "incorrect fail message")
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
        
        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        Assertions.assertTrue(eventMap["fail"].isNullOrEmpty(), "incorrect fail message")
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
        
        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        Assertions.assertTrue(eventMap["fail"].isNullOrEmpty(), "incorrect fail message")
    }

    @Test
    internal fun negative_message_badUrl(){
        var map = defaultMap();
        var message = buildTestMessage(map);

        val e = Assertions.assertThrows(IllegalArgumentException::class.java) {
            testFnDebatcher.process(message,mockContext);
        }
        
        Assertions.assertEquals("Azure message had bad URL for the file! DUMMY_URL", e.message)
        
        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        Assertions.assertTrue(eventMap["fail"].isNullOrEmpty(), "incorrect fail message")
    }

    @Test
    internal fun negative_file_missingFile(){
        var map = defaultMap(url="DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/test-not-there")
        var message = buildTestMessage(map);
        
        val e = Assertions.assertThrows(IllegalArgumentException::class.java) {
            testFnDebatcher.process(message,mockContext);
        }
        
        Assertions.assertEquals("File missing in Azure! DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/test-not-there", e.message)
        
        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        Assertions.assertTrue(eventMap["fail"].isNullOrEmpty(), "incorrect fail message")
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //bad file, create fail message

    @Test
    internal fun negative_file_unableToZip(){
        copyToIngest("test-upload.csv")
        var map = defaultMap(url="DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/test-upload.csv", contentType="application/zip",id="bad_zip")
        var message = buildTestMessage(map);
        
        testFnDebatcher.process(message,mockContext);

        Assertions.assertTrue(ingestDir.list().isEmpty(), "ingest file not empty!")
        Assertions.assertFalse(File(processDir,"bad_zip/test-upload.csv").exists(), "file remained in processed!")
        Assertions.assertTrue(File(errorDir,"bad_zip/test-upload.csv").exists(), "file not put in error!")

        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        var failMessageList = eventMap["fail"]
        Assertions.assertFalse(failMessageList.isNullOrEmpty(), "missing fail message")
        Assertions.assertEquals(1, failMessageList?.size, "too many fail messages")
    }

    @Test
    internal fun negative_file_emptyZip(){
        copyToIngest("test-empty.zip")
        var map = defaultMap(url="DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/test-empty.zip", contentType="application/zip",id="empty_zip")
        var message = buildTestMessage(map);
        
        testFnDebatcher.process(message,mockContext);

        Assertions.assertTrue(ingestDir.list().isEmpty(), "ingest file not empty!")
        Assertions.assertFalse(File(processDir,"empty_zip/test-empty.zip").exists(), "file remained in processed!")
        Assertions.assertTrue(File(errorDir,"empty_zip/test-empty.zip").exists(), "file not put in error!")

        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        var failMessageList = eventMap["fail"]
        Assertions.assertFalse(failMessageList.isNullOrEmpty(), "missing fail message")
        Assertions.assertEquals(1, failMessageList?.size, "too many fail messages")
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //helper functions

    private fun copyToIngest(fileName:String){
        var localFileIn = File("src/test/resources/testfiles",fileName);
        var localFileOut = File(ingestDir,fileName);
        
        localFileOut.parentFile.mkdirs()
        localFileIn.copyTo(localFileOut);
    }

    private fun doesTestFileExist(i: InvocationOnMock):Boolean{
        val container:String = i.getArgument(0);
        val relativePath:String = i.getArgument(1);

        var localFile = File(File(outputParentDir, container),relativePath);
        return localFile.exists();
    }
    
    private fun getMetadata(i: InvocationOnMock):Map<String,String>{
        val container:String = i.getArgument(0);
        val relativePath:String = i.getArgument(1);

        var localFile = File(File(outputParentDir, container),relativePath);

        var meta = mapOf("exists" to ""+localFile.exists(), "filepath" to localFile.absolutePath)
        return meta;
    }
    
    private fun moveTestFile(i: InvocationOnMock):String{
        val containerIn:String = i.getArgument(0);
        val relativePathIn:String = i.getArgument(1);
        val containerOut:String = i.getArgument(2);
        val relativePathOut:String = i.getArgument(3);

        var localFileIn = File(File(outputParentDir, containerIn),relativePathIn);
        var localFileOut = File(File(outputParentDir, containerOut),relativePathOut);

        localFileOut.parentFile.mkdirs()

        localFileIn.copyTo(localFileOut);

        localFileIn.delete()

        return localFileOut.absolutePath;
    }
    
    private fun openInputStream(i: InvocationOnMock):InputStream{
        val container:String = i.getArgument(0);
        val relativePath:String = i.getArgument(1);

        var localFile = File(File(outputParentDir, container),relativePath);
        return FileInputStream(localFile);
    }
    
    private fun openOutputStream(i: InvocationOnMock):Pair<OutputStream,String>{
        val container:String = i.getArgument(0);
        val relativePath:String = i.getArgument(1);

        var localFile = File(File(outputParentDir, container),relativePath);
        localFile.parentFile.mkdirs();
        return Pair(FileOutputStream(localFile),localFile.absolutePath);
    }
    
    private fun addEvent(i: InvocationOnMock){
        val hubName:String = i.getArgument(0);
        val message:String = i.getArgument(1);

        commonAddEvent(hubName, listOf(message))
    }
    
    private fun addEvents(i: InvocationOnMock){
        val hubName:String = i.getArgument(0);
        val messages:List<String> = i.getArgument(1);
        
        commonAddEvent(hubName, messages)
    }

    private fun commonAddEvent(hubName:String, messages:List<String>){
        for(message in messages){
            println("[event][$hubName] $message")
        }

        var list = eventMap.getOrDefault(hubName, mutableListOf());
        eventMap.put(hubName, list)
        list.addAll(messages);
    }

    
    fun defaultMap(id:String = "DUMMY_ID",url:String = "DUMMY_URL", contentType:String = "DUMMY_CONTENT_TYPE"): MutableMap<*,*>{
        val mapData = mutableMapOf("url" to url, "contentType" to contentType, "extraField" to "DUMMY_EXTRA_FIELD");
        return mutableMapOf("eventType" to "Microsoft.Storage.BlobCreated", "id" to id, "data" to mapData, "extraField" to "DUMMY_EXTRA_FIELD");
    }

    fun buildTestMessage(mapObj:Map<*,*>): String { 
        val message = recursiveBuildMessageFromMap(mapObj);
        return "[$message]"
    }

    private fun recursiveBuildMessageFromMap(map:Map<*,*>):String{
        return map.map { (k,v)-> 
            var value = if(v is Map<*,*>){recursiveBuildMessageFromMap(v)}else{"\"$v\""};
            var combined="\"$k\":$value";
            combined
        }
        .joinToString(separator=",",prefix="{",postfix="}") 
    }

    fun mockContext(): ExecutionContext{
        val mockContext : ExecutionContext = Mockito.mock(ExecutionContext::class.java);
        val logger : Logger = Mockito.mock(Logger::class.java);

        Mockito.`when`(mockContext.logger).thenReturn(logger)
        Mockito.`when`(logger.info(Mockito.anyString())).thenAnswer(::loggerInvocation)
        Mockito.`when`(logger.warning(Mockito.anyString())).thenAnswer(::loggerInvocation)
        return mockContext
    }

    private fun loggerInvocation(i: InvocationOnMock){
        val toLog:String = i.getArgument(0);
        println("[log] $toLog");
    }
}