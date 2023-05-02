package gov.cdc.dex.csv.functions

import com.microsoft.azure.functions.ExecutionContext
import java.util.logging.Logger;
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock;

import gov.cdc.dex.csv.services.BlobService

internal class Unit_FnDecompressor {

    private val mockBlobService : BlobService = Mockito.mock(BlobService::class.java);
    private val mockContext : ExecutionContext = Mockito.mock(ExecutionContext::class.java);
    private val logger : Logger = Mockito.mock(Logger::class.java);

    private val testFnDebatcher : FnDecompressor = FnDecompressor(mockBlobService);

    @BeforeEach
    fun initiateMocks(){
        Mockito.`when`(mockContext.logger).thenReturn(logger)
        Mockito.`when`(logger.info(Mockito.anyString())).thenAnswer(this::loggerInvocation)
    }

    private fun loggerInvocation(i: InvocationOnMock){
        val toLog:String = i.getArgument(0);
        println(toLog);
        //TODO maybe do something with this in the tests
    }

    @Test
    internal fun happyPath_singleCsv(){
        //TODO
    }

    @Test
    internal fun happyPath_zip(){
        //TODO
    }

    @Test
    internal fun negative_message_empty(){
        Assertions.assertThrows(IllegalArgumentException::class.java) {
            testFnDebatcher.process("",mockContext);
        }
    }

    @Test
    internal fun negative_message_badFormat(){
        Assertions.assertThrows(com.google.gson.JsonSyntaxException::class.java) {
            testFnDebatcher.process("][",mockContext);
        }
    }

    @Test
    internal fun negative_message_missingEventType(){
        var map = defaultMap();
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

        Assertions.assertThrows(IllegalArgumentException::class.java) {
            testFnDebatcher.process(message,mockContext);
        }
    }

    @Test
    internal fun negative_message_missingData(){
        var map = defaultMap();
        map.remove("data");
        var message = buildTestMessage(map);

        Assertions.assertThrows(IllegalArgumentException::class.java) {
            testFnDebatcher.process(message,mockContext);
        }
    }

    @Test
    internal fun negative_message_missingContentType(){
        var map = defaultMap();
        var dataMap = map["data"] as MutableMap<*,*>;
        dataMap.remove("contentType");
        var message = buildTestMessage(map);

        Assertions.assertThrows(IllegalArgumentException::class.java) {
            testFnDebatcher.process(message,mockContext);
        }
    }

    @Test
    internal fun negative_message_missingUrl(){
        var map = defaultMap();
        var dataMap = map["data"] as MutableMap<*,*>;
        dataMap.remove("url");
        var message = buildTestMessage(map);

        Assertions.assertThrows(IllegalArgumentException::class.java) {
            testFnDebatcher.process(message,mockContext);
        }
    }

    @Test
    internal fun negative_message_badUrl(){
        var map = defaultMap();
        var dataMap = map["data"] as MutableMap<String,String>;
        dataMap["url"] = "something bad";
        var message = buildTestMessage(map);

        //TODO
        // Assertions.assertThrows(IllegalArgumentException::class.java) {
        //     testFnDebatcher.process(message,mockContext);
        // }
    }

    @Test
    internal fun negative_integration_unableToConnectToStorage(){
        //TODO
    }

    @Test
    internal fun negative_file_missingFile(){
        //TODO
    }

    @Test
    internal fun negative_file_unableToZip(){
        //TODO
    }

    private fun defaultMap(): MutableMap<*,*>{
        val mapData = mutableMapOf("url" to "DUMMY_URL", "contentType" to "DUMMY_CONTENT_TYPE", "extraField" to "DUMMY_EXTRA_FIELD");
        return mutableMapOf("eventType" to "Microsoft.Storage.BlobCreated", "id" to "DUMMY_ID", "data" to mapData);
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