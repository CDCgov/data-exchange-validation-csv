package gov.cdc.dex.csv.test.utils

import com.microsoft.azure.functions.ExecutionContext
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import java.util.logging.Logger


fun defaultMap(id:String = "DUMMY_ID",url:String = "DUMMY_URL", contentType:String = "DUMMY_CONTENT_TYPE"): MutableMap<*,*>{
    val mapData = mutableMapOf("url" to url, "contentType" to contentType, "extraField" to "DUMMY_EXTRA_FIELD");
    return mutableMapOf("eventType" to "Microsoft.Storage.BlobCreated", "id" to id, "data" to mapData, "extraField" to "DUMMY_EXTRA_FIELD");
}

fun buildTestMessage(mapObj:Map<*,*>): String { 
    val message = recursiveBuildMessageFromMap(mapObj);
    return "[[$message]]"
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
    return mockContext
}

private fun loggerInvocation(i: InvocationOnMock){
    val toLog:String = i.getArgument(0);
    println("[log] $toLog");
}