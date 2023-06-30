package gov.cdc.dex.csv.functions

import com.azure.storage.blob.BlobClientBuilder
import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.azure.functions.annotation.HttpTrigger
import com.microsoft.azure.functions.annotation.AuthorizationLevel
import com.microsoft.azure.functions.HttpResponseMessage
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage
import com.microsoft.azure.functions.ExecutionContext
import com.microsoft.azure.functions.HttpStatus
import com.microsoft.durabletask.azurefunctions.DurableClientInput
import com.microsoft.durabletask.azurefunctions.DurableClientContext
import com.microsoft.durabletask.Task
import com.microsoft.durabletask.TaskOrchestrationContext
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;

import java.util.Optional
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.Callable
import java.util.logging.Level
import java.io.InputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

class DummyFileSplitCompare {
    private fun commonOpenFileStream(blobUrl:String):InputStream{
        val connectionString = System.getenv("BlobConnection")
        
        val client = BlobClientBuilder().connectionString(connectionString).endpoint(blobUrl).buildClient()
        return client.openInputStream()
    }

    private fun commonProcessBatch(batchContent:String):String{
        return batchContent.split("\n").map{it.first()}.fold(""){agg,element -> agg+element}
    }
    
    @FunctionName("Dummy_Single")
    fun Dummy_Single(
        @HttpTrigger(name = "req", 
            methods = [HttpMethod.GET], 
            authLevel = AuthorizationLevel.ANONYMOUS)
        request:HttpRequestMessage<Optional<String>>,
        context: ExecutionContext
    ):HttpResponseMessage  {
        try{
            context.logger.log(Level.INFO,"Dummy_Single started")
            val startTime = LocalDateTime.now()
            val batchSize = request.queryParameters["batchSize"]?.toInt()?:1
            val numberThreads = request.queryParameters["numberThreads"]?.toInt()?:10
            val fileUrl = request.body.get()
            val inputStream = commonOpenFileStream(fileUrl)
            
            val futures = mutableListOf<Future<String>>()
            BufferedReader(InputStreamReader(inputStream)).use{
                
                val headerLine = it.readLine()
                val service = Executors.newFixedThreadPool(numberThreads)
                
                var currentInBatch = 0
                var stringBuilder = StringBuilder()
                
                var nextLine = it.readLine()
                while(nextLine != null) {
                    currentInBatch++;
                    stringBuilder.append("\n").append(nextLine);
                    if(currentInBatch >= batchSize) {
                        val batchSubmit = headerLine + stringBuilder.toString()
                        futures.add(service.submit(Callable<String>{commonProcessBatch(batchSubmit)}))
                        
                        stringBuilder.clear()
                        currentInBatch = 0
                    }
                    nextLine = it.readLine()
                }
                if(stringBuilder.isNotEmpty()){
                    val batchSubmit = headerLine + stringBuilder.toString()
                    futures.add(service.submit(Callable<String>{commonProcessBatch(batchSubmit)}))
                }
            }
        
        
            var stillRunning = futures.size
            while(stillRunning > 0) {
                stillRunning = futures.filter{!it.isDone}.size
            }
            val output = futures.map{it.get()}.toString()
            
            val endTime = LocalDateTime.now()
            val timeDiff = ChronoUnit.MILLIS.between(startTime, endTime)
            context.logger.log(Level.INFO,"Dummy_Single finished, time $timeDiff millis")
            return request.createResponseBuilder(HttpStatus.OK).body("Finished, time $timeDiff millis\n\n$output").build()
        }catch(e:Exception){
            context.logger.log(Level.SEVERE,"Dummy_Single error",e)
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body(e).build()
        }
    }
    
    @FunctionName("Dummy_Multi_Router")
    fun Dummy_Multi_Router(
        @HttpTrigger(name = "req", 
        methods = [HttpMethod.GET], 
            authLevel = AuthorizationLevel.ANONYMOUS)
        request:HttpRequestMessage<Optional<String>>,
        @DurableClientInput(name = "durableContext") durableContext:DurableClientContext,
        context: ExecutionContext
    ):HttpResponseMessage  {
        try{
            context.logger.log(Level.INFO,"Dummy_Multi_Router started")
            val startTime = LocalDateTime.now()
            val batchSize = request.queryParameters["batchSize"]?.toInt()?:1
            val fileUrl = request.body.get()

            val orchInput = DumOrchInput(batchSize, fileUrl, startTime.toString())
            
            val instanceId = durableContext.client.scheduleNewOrchestrationInstance("Dummy_Multi_Orchestrator",orchInput);

            val endTime = LocalDateTime.now()
            val timeDiff = ChronoUnit.MILLIS.between(startTime, endTime)
            context.logger.log(Level.INFO,"Dummy_Multi_Router finished, time $timeDiff millis")
            return durableContext.createCheckStatusResponse(request, instanceId);
        }catch(e:Exception){
            context.logger.log(Level.SEVERE,"Dummy_Multi_Router error",e)
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body(e).build()
        }
    }
    
    @FunctionName("Dummy_Multi_Orchestrator")
    fun Dummy_Multi_Orchestrator(
        @DurableOrchestrationTrigger(name = "taskOrchestrationContext") taskContext:TaskOrchestrationContext,
        functionContext:ExecutionContext
    ):DumOrchOutput  {
        orchlog(Level.INFO,"Dummy_Multi_Orchestrator started", taskContext=taskContext, functionContext=functionContext)
        val input = taskContext.getInput(DumOrchInput::class.java)
        try{

            val inputStream = commonOpenFileStream(input.fileUrl)
            
            val parallelTasks:MutableList<Task<DumActivityOutput>> = mutableListOf()
            BufferedReader(InputStreamReader(inputStream)).use{
                
                val headerLine = it.readLine()

                var currentInBatch = 0
                var stringBuilder = StringBuilder()
                
                var nextLine = it.readLine()
                while(nextLine != null) {
                    currentInBatch++;
                    stringBuilder.append("\n").append(nextLine);
                    if(currentInBatch >= input.batchSize) {
                        val batchSubmit = headerLine + stringBuilder.toString()
                        parallelTasks.add(taskContext.callActivity("Dummy_Multi_Activity", DumActivityInput(batchSubmit), DumActivityOutput::class.java))

                        stringBuilder.clear()
                        currentInBatch = 0
                    }
                    nextLine = it.readLine()
                }
                if(stringBuilder.isNotEmpty()){
                    val batchSubmit = headerLine + stringBuilder.toString()
                    parallelTasks.add(taskContext.callActivity("Dummy_Multi_Activity", DumActivityInput(batchSubmit), DumActivityOutput::class.java))
                }

            }

            orchlog(Level.INFO,"Dummy_Multi_Orchestrator waiting for tasks", taskContext=taskContext, functionContext=functionContext)

            val subOutputs = taskContext.allOf(parallelTasks).await()
            val out = subOutputs.map{it.outputMessage}.toString()

            val startTime = LocalDateTime.parse(input.startTime)
            val endTime = LocalDateTime.now()
            val timeDiff = ChronoUnit.MILLIS.between(startTime, endTime)
            orchlog(Level.INFO,"Dummy_Multi_Orchestrator finished, time $timeDiff millis", taskContext=taskContext, functionContext=functionContext)
            
            return DumOrchOutput(input, out, endTime.toString())
            
        }catch(e:com.microsoft.durabletask.OrchestratorBlockedException){
            throw e
        }catch(e:Exception){
            orchlog(Level.SEVERE,"Dummy_Multi_Router error",e, taskContext=taskContext, functionContext=functionContext)
            return DumOrchOutput(input, "ERROR ${e.localizedMessage}", LocalDateTime.now().toString())
        }
    }
    
    @FunctionName("Dummy_Multi_Activity")
    fun Dummy_Multi_Activity(
        @DurableActivityTrigger(name = "input") input: DumActivityInput, 
        context: ExecutionContext 
    ):DumActivityOutput{
        val out=commonProcessBatch(input.batchContent)

        return DumActivityOutput(input, out)
    }

    

    private fun orchlog(
        level:Level, msg:String, thrown:Throwable?=null, taskContext:TaskOrchestrationContext, functionContext:ExecutionContext
    ){
        var toLog = if (taskContext.getIsReplaying()) {"-REPLAY-"}else{""}
        toLog = toLog + msg
        if(thrown == null){
            functionContext.logger.log(level, toLog);
        } else{
            functionContext.logger.log(level, toLog, thrown);
        }
        
    }
}

data class DumOrchInput(
    val batchSize       : Int,
    val fileUrl         : String,
    val startTime       : String
)

data class DumOrchOutput(
    val intput          : DumOrchInput,
    val outputMessage   : String,
    val endTime         : String
)

data class DumActivityInput(
    val batchContent    : String
)

data class DumActivityOutput(
    val input           : DumActivityInput,
    val outputMessage   : String
)