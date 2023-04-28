package gov.cdc.dex.csv.decompressor

import com.microsoft.azure.functions.ExecutionContext
import java.util.logging.Logger;
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock;

internal class Unit_FnDecompressor {

    private val testFnDebatcher : FnDecompressor = FnDecompressor();
    private val context : ExecutionContext = Mockito.mock(ExecutionContext::class.java);
    private val logger : Logger = Mockito.mock(Logger::class.java);

    @BeforeEach
    internal fun initiateMocks(){
        Mockito.`when`(context.logger).thenReturn(logger)
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
        //TODO
        val exception = Assertions.assertThrows(Exception::class.java) {
            testFnDebatcher.eventHubProcessor("",context);
        }
    }

    @Test
    internal fun negative_message_missingFields(){
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