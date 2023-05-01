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
    internal fun negative_message_missingType(){
        testFnDebatcher.process("[[{\"id\":\"DUMMY_ID\",\"data\":{\"url\":\"DUMMY_URL\",\"extraField\":\"DUMMY_EXTRA_FIELD\"},\"eventTime\":\"DUMMY_EVENT_TIME\",\"extraField\":\"DUMMY_EXTRA_FIELD\"}]]",mockContext);
        //TODO make sure that nothing happens
    }

    @Test
    internal fun negative_message_missingUrl(){
        Assertions.assertThrows(IllegalArgumentException::class.java) {
            testFnDebatcher.process("[[{\"eventType\":\"Microsoft.Storage.BlobCreated\",\"id\":\"DUMMY_ID\",\"data\":{\"extraField\":\"DUMMY_EXTRA_FIELD\"},\"eventTime\":\"DUMMY_EVENT_TIME\",\"extraField\":\"DUMMY_EXTRA_FIELD\"}]]",mockContext);
        }
        //TODO
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

    private fun buildValidTestMessage(url: String): String { 
        return "[[{\"eventType\":\"Microsoft.Storage.BlobCreated\",\"id\":\"DUMMY_ID\",\"data\":{\"url\":\"$url\",\"extraField\":\"DUMMY_EXTRA_FIELD\"},\"eventTime\":\"DUMMY_EVENT_TIME\",\"extraField\":\"DUMMY_EXTRA_FIELD\"}]]"
    }
}