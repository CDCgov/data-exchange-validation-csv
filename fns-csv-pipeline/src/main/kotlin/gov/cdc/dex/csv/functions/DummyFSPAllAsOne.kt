package gov.cdc.dex.csv.functions

import com.azure.storage.blob.BlobClientBuilder
import com.azure.core.util.BinaryData
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
import com.microsoft.durabletask.azurefunctions.HttpManagementPayload
import com.fasterxml.jackson.annotation.JsonProperty

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
import java.time.format.DateTimeFormatter
import kotlin.random.Random

class DummyFSPAllAsOne {
    
    @FunctionName("Dummy_Router")
    fun Dummy_Multi_Router(
        @HttpTrigger(name = "req", 
        methods = [HttpMethod.GET], 
            authLevel = AuthorizationLevel.ANONYMOUS)
        request:HttpRequestMessage<Optional<String>>,
        @DurableClientInput(name = "durableContext") durableContext:DurableClientContext,
        context: ExecutionContext
    ):HttpResponseMessage  {
        try{
            val startTime = LocalDateTime.now()
            context.logger.log(Level.INFO,"Dummy_Multi_Router started")

            val orchInputs = mutableListOf<DumOrchInput>()
            for(delayTime in listOf(0)){
                // for(delayTime in listOf(0 ,1000,10000)){
                // var fileUrl = "https://dexcsvdata001.blob.core.windows.net/testing/orchestrator-split-poc/test-upload-small.csv"
                // for(batchSize in listOf(1,100)){
                //     for(numThreads in listOf(1,10)){
                //         orchInputs.add(DumOrchInput(batchSize, numThreads, delayTime, fileUrl, LocalDateTime.now().toString()))
                //     }
                // }
                // fileUrl = "https://dexcsvdata001.blob.core.windows.net/testing/orchestrator-split-poc/test-upload-mid.csv"
                // for(batchSize in listOf(1,100,10000)){
                //     for(numThreads in listOf(1,10,100)){
                //         orchInputs.add(DumOrchInput(batchSize, numThreads, delayTime, fileUrl, LocalDateTime.now().toString()))
                //     }
                // }
                var fileUrl = "https://dexcsvdata001.blob.core.windows.net/testing/orchestrator-split-poc/test-upload-big.csv"
                for(batchSize in listOf(1000)){
                    for(numThreads in listOf(100)){
                        // for(batchSize in listOf(100,10000)){
                        //     for(numThreads in listOf(1,10,100)){
                        orchInputs.add(DumOrchInput(batchSize, numThreads, delayTime, fileUrl, LocalDateTime.now().toString()))
                    }
                }
                fileUrl = "https://dexcsvdata001.blob.core.windows.net/testing/orchestrator-split-poc/test-upload-bigger.csv"
                for(batchSize in listOf(1000)){
                    for(numThreads in listOf(100)){
                        // for(batchSize in listOf(10000)){
                        //     for(numThreads in listOf(10,100)){
                        orchInputs.add(DumOrchInput(batchSize, numThreads, delayTime, fileUrl, LocalDateTime.now().toString()))
                    }
                }
            }

            val bodyList = mutableListOf<ResponseBodyItem>()
            for(orchInput in orchInputs){
                val m_instanceId = durableContext.client.scheduleNewOrchestrationInstance("Dummy_Multi_Orchestrator",orchInput);
                val s_instanceId = durableContext.client.scheduleNewOrchestrationInstance("Dummy_Single_Orchestrator",orchInput);

                val m_links = durableContext.createHttpManagementPayload(request, m_instanceId)
                val s_links = durableContext.createHttpManagementPayload(request, s_instanceId)

                bodyList.add(ResponseBodyItem(m_links, "multi", orchInput))
                bodyList.add(ResponseBodyItem(s_links, "single", orchInput))
            }


            val endTime = LocalDateTime.now()
            val timeDiff = ChronoUnit.MILLIS.between(startTime, endTime)
            context.logger.log(Level.INFO,"Dummy_Multi_Router finished, time $timeDiff millis")


            return request.createResponseBuilder(HttpStatus.ACCEPTED)
                .header("Content-Type", "application/json")
                .body(bodyList)
                .build();
        }catch(e:Exception){
            context.logger.log(Level.SEVERE,"Dummy_Multi_Router error",e)
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body(e).build()
        }
    }

    
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    
    @FunctionName("Dummy_Single_Orchestrator")
    fun Dummy_Single_Orchestrator(
        @DurableOrchestrationTrigger(name = "taskOrchestrationContext") taskContext:TaskOrchestrationContext,
        functionContext:ExecutionContext
    ):DumOrchOutput  {
        orchlog(Level.INFO,"Dummy_Single_Orchestrator started", taskContext=taskContext, functionContext=functionContext)
        val input = try{
            taskContext.getInput(DumOrchInput::class.java)
        }catch(e:Exception){
            orchlog(Level.SEVERE,"Dummy_Single_Orchestrator unable to parse input! error ${e.localizedMessage}",e, taskContext=taskContext, functionContext=functionContext)
            throw e
        }

        try{
            val output = taskContext.callActivity("Dummy_Single_Activity", DumActivityInput(input.fileUrl, input), DumActivityOutput::class.java).await()
        
            val startTime = LocalDateTime.parse(input.startTime)
        
            val endTime = LocalDateTime.now()
            val timeDiff = ChronoUnit.MILLIS.between(startTime, endTime)
            orchlog(Level.INFO,"Dummy_Single_Orchestrator finished, time $timeDiff millis", taskContext=taskContext, functionContext=functionContext)
            
            return DumOrchOutput(timeDiff, null, input, null, null, endTime.toString())
            
        }catch(e:com.microsoft.durabletask.OrchestratorBlockedException){
            throw e
        }catch(e:Exception){
            orchlog(Level.SEVERE,"Dummy_Single_Orchestrator error ${e.localizedMessage}",e, taskContext=taskContext, functionContext=functionContext)
            return DumOrchOutput(null, null, input, null, "ERROR Dummy_Single_Orchestrator ${e.localizedMessage}", null)
        }
    }

    @FunctionName("Dummy_Single_Activity")
    fun Dummy_Single_Activity(
        @DurableActivityTrigger(name = "input") input: DumActivityInput, 
        context: ExecutionContext 
    ):DumActivityOutput{
        context.logger.log(Level.INFO,"Dummy_Single_Activity started ${input}")
        val commonOutput = commonMultithread(input.orchInput){commonProcessBatch(it, input.orchInput.delayTime)}
        return DumActivityOutput(input, null, commonOutput.timeDiff)
    }
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        
    
    @FunctionName("Dummy_Multi_Orchestrator")
    fun Dummy_Multi_Orchestrator(
        @DurableOrchestrationTrigger(name = "taskOrchestrationContext") taskContext:TaskOrchestrationContext,
        functionContext:ExecutionContext
    ):DumOrchOutput  {
        orchlog(Level.INFO,"Dummy_Multi_Orchestrator started", taskContext=taskContext, functionContext=functionContext)
        val input = try{
            taskContext.getInput(DumOrchInput::class.java)
        }catch(e:Exception){
            orchlog(Level.SEVERE,"Dummy_Multi_Orchestrator unable to parse input! error ${e.localizedMessage}",e, taskContext=taskContext, functionContext=functionContext)
            throw e
        }


        try{
            val splitOutput = taskContext.callActivity("Dummy_Multi_Splitter", DumActivityInput("", input), DumActivityOutput::class.java).await()
        
            val parallelTasks:MutableList<Task<DumActivityOutput>> = mutableListOf()
            splitOutput.outputList?.forEach{
                parallelTasks.add(taskContext.callActivity("Dummy_Multi_Activity", DumActivityInput(it, input), DumActivityOutput::class.java))
            }
        
            orchlog(Level.INFO,"Dummy_Multi_Orchestrator waiting for tasks", taskContext=taskContext, functionContext=functionContext)
            
            val subOutputs = taskContext.allOf(parallelTasks).await()
        
            val startTime = LocalDateTime.parse(input.startTime)
        
            val endTime = LocalDateTime.now()
            val timeDiff = ChronoUnit.MILLIS.between(startTime, endTime)
            orchlog(Level.INFO,"Dummy_Multi_Orchestrator finished, time $timeDiff millis", taskContext=taskContext, functionContext=functionContext)
            
            return DumOrchOutput(timeDiff, splitOutput.timeDiff, input, splitOutput, null, endTime.toString())
            
        }catch(e:com.microsoft.durabletask.OrchestratorBlockedException){
            throw e
        }catch(e:Exception){
            orchlog(Level.SEVERE,"Dummy_Multi_Orchestrator error ${e.localizedMessage}",e, taskContext=taskContext, functionContext=functionContext)
            return DumOrchOutput(null, null, input, null, "ERROR Dummy_Multi_Activity ${e.localizedMessage}", null)
        }
    }

    
    
    @FunctionName("Dummy_Multi_Splitter")
    fun Dummy_Multi_Splitter(
        @DurableActivityTrigger(name = "input") input: DumActivityInput, 
        context: ExecutionContext 
    ):DumActivityOutput{
        context.logger.log(Level.INFO,"Dummy_Multi_Splitter started ${input}")
        val commonOutput = commonMultithread(input.orchInput){storeBatch(it)}

        return DumActivityOutput(input, commonOutput.outputList, commonOutput.timeDiff)
    }
    
    @FunctionName("Dummy_Multi_Activity")
    fun Dummy_Multi_Activity(
        @DurableActivityTrigger(name = "input") input: DumActivityInput, 
        context: ExecutionContext 
    ):DumActivityOutput{
            val content = retrieveBatch(input.batchKey)
            val out=commonProcessBatch(content, input.orchInput.delayTime)
            return DumActivityOutput(input, null, null)
    }
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


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

    private fun commonOpenFileStream(blobUrl:String):InputStream{
        val connectionString = System.getenv("BlobConnection")
        
        val client = BlobClientBuilder().connectionString(connectionString).endpoint(blobUrl).buildClient()
        return client.openInputStream()
    }

    private fun commonProcessBatch(batchContent:String, delayTime:Int):String{
        Thread.sleep(delayTime.toLong())
        return batchContent.length.toString()
    }

    private fun storeBatch(content:String):String{

        val connectionString = System.getenv("BlobConnection")
        val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-SSSSSS"))
        val rand = Random.nextInt(0,999)
        val batchKey = "https://dexcsvdata001.blob.core.windows.net/processed/test-split/${timestamp}.R${rand}"
        
        val client = BlobClientBuilder().connectionString(connectionString).endpoint(batchKey).buildClient()
        client.blockBlobClient.upload(BinaryData.fromString(content))
        return batchKey
    }
    
    private fun retrieveBatch(batchKey:String):String{

        val connectionString = System.getenv("BlobConnection")
        
        val client = BlobClientBuilder().connectionString(connectionString).endpoint(batchKey).buildClient()
        return client.downloadContent().toString()
    }

    private fun commonMultithread(
        input: DumOrchInput,
        threadRun: (s:String)->String
    ):CommonMultithreadOutput{
        val startTime = LocalDateTime.now()
        val inputStream = commonOpenFileStream(input.fileUrl)
        val batchSize = input.batchSize
        val numberThreads = input.numberThreads
        
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
                    futures.add(service.submit(Callable<String>{threadRun(batchSubmit)}))
                    
                    stringBuilder.clear()
                    currentInBatch = 0
                }
                nextLine = it.readLine()
            }
            if(stringBuilder.isNotEmpty()){
                val batchSubmit = headerLine + stringBuilder.toString()
                futures.add(service.submit(Callable<String>{threadRun(batchSubmit)}))
            }
        }
    
    
        var stillRunning = futures.size
        while(stillRunning > 0) {
            stillRunning = futures.filter{!it.isDone}.size
        }

        val outputList = futures.map{it.get()}
        
        val endTime = LocalDateTime.now()
        val timeDiff = ChronoUnit.MILLIS.between(startTime, endTime)

        return CommonMultithreadOutput(outputList, timeDiff)
    }
}
data class ResponseBodyItem(
    val statusLinks : HttpManagementPayload,
    val sOrM        : String,
    val orchInput   : DumOrchInput
)

data class DumOrchInput(
    @JsonProperty("batchSize")      val batchSize       : Int,
    @JsonProperty("numberThreads")  val numberThreads   : Int,
    @JsonProperty("delayTime")      val delayTime       : Int,
    @JsonProperty("fileUrl")        val fileUrl         : String,
    @JsonProperty("startTime")      val startTime       : String
)

data class DumOrchOutput(
    @JsonProperty("timeDiff")       val timeDiff        : Long?,
    @JsonProperty("timeDiffSplit")  val timeDiffSplit   : Long?,
    @JsonProperty("input")          val input           : DumOrchInput,
    @JsonProperty("splitOutput")    val splitOutput     : DumActivityOutput?,
    @JsonProperty("errorMessage")   val errorMessage    : String?,
    @JsonProperty("endTime")        val endTime         : String?
)

data class DumActivityInput(
    @JsonProperty("batchKey")       val batchKey        : String,
    @JsonProperty("orchInput")      val orchInput       : DumOrchInput
)

data class DumActivityOutput(
    @JsonProperty("input")          val input           : DumActivityInput,
    @JsonProperty("outputList")     val outputList      : List<String>?,
    @JsonProperty("timeDiff")       val timeDiff        : Long?
)

data class CommonMultithreadOutput(
    val outputList      : List<String>,
    val timeDiff        : Long
)