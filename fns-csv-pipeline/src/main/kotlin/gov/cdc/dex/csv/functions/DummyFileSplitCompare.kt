// package gov.cdc.dex.csv.functions

// import com.azure.storage.blob.BlobClientBuilder
// import com.azure.core.util.BinaryData
// import com.microsoft.azure.functions.annotation.FunctionName
// import com.microsoft.azure.functions.annotation.HttpTrigger
// import com.microsoft.azure.functions.annotation.AuthorizationLevel
// import com.microsoft.azure.functions.HttpResponseMessage
// import com.microsoft.azure.functions.HttpMethod;
// import com.microsoft.azure.functions.HttpRequestMessage
// import com.microsoft.azure.functions.ExecutionContext
// import com.microsoft.azure.functions.HttpStatus
// import com.microsoft.durabletask.azurefunctions.DurableClientInput
// import com.microsoft.durabletask.azurefunctions.DurableClientContext
// import com.microsoft.durabletask.Task
// import com.microsoft.durabletask.TaskOrchestrationContext
// import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger
// import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;
// import com.fasterxml.jackson.annotation.JsonProperty

// import java.util.Optional
// import java.util.concurrent.Executors
// import java.util.concurrent.Future
// import java.util.concurrent.Callable
// import java.util.logging.Level
// import java.io.InputStream
// import java.io.BufferedReader
// import java.io.InputStreamReader
// import java.time.LocalDateTime
// import java.time.temporal.ChronoUnit
// import java.time.format.DateTimeFormatter
// import kotlin.random.Random

// class DummyFileSplitCompare {
//     private fun commonOpenFileStream(blobUrl:String):InputStream{
//         val connectionString = System.getenv("BlobConnection")
        
//         val client = BlobClientBuilder().connectionString(connectionString).endpoint(blobUrl).buildClient()
//         return client.openInputStream()
//     }

//     private fun commonProcessBatch(batchContent:String, delayTime:Long):String{
//         Thread.sleep(delayTime)
//         return batchContent.split("\n").map{it.first()}.fold(""){agg,element -> agg+element}
//     }
    
//     @FunctionName("Dummy_Single")
//     fun Dummy_Single(
//         @HttpTrigger(name = "req", 
//             methods = [HttpMethod.GET], 
//             authLevel = AuthorizationLevel.ANONYMOUS)
//         request:HttpRequestMessage<Optional<String>>,
//         context: ExecutionContext
//     ):HttpResponseMessage  {
//         try{
//             context.logger.log(Level.INFO,"Dummy_Single started")
//             val startTime = LocalDateTime.now()
//             val batchSize = request.queryParameters["batchSize"]?.toInt()?:1
//             val numberThreads = request.queryParameters["numberThreads"]?.toInt()?:10
//             val delayTime = request.queryParameters["delayTime"]?.toLong()?:0
//             val fileUrl = request.body.get()
//             val inputStream = commonOpenFileStream(fileUrl)
            
//             val futures = mutableListOf<Future<String>>()
//             BufferedReader(InputStreamReader(inputStream)).use{
                
//                 val headerLine = it.readLine()
//                 val service = Executors.newFixedThreadPool(numberThreads)
                
//                 var currentInBatch = 0
//                 var stringBuilder = StringBuilder()
                
//                 var nextLine = it.readLine()
//                 while(nextLine != null) {
//                     currentInBatch++;
//                     stringBuilder.append("\n").append(nextLine);
//                     if(currentInBatch >= batchSize) {
//                         val batchSubmit = headerLine + stringBuilder.toString()
//                         futures.add(service.submit(Callable<String>{commonProcessBatch(batchSubmit, delayTime)}))
                        
//                         stringBuilder.clear()
//                         currentInBatch = 0
//                     }
//                     nextLine = it.readLine()
//                 }
//                 if(stringBuilder.isNotEmpty()){
//                     val batchSubmit = headerLine + stringBuilder.toString()
//                     futures.add(service.submit(Callable<String>{commonProcessBatch(batchSubmit, delayTime)}))
//                 }
//             }
        
        
//             var stillRunning = futures.size
//             while(stillRunning > 0) {
//                 stillRunning = futures.filter{!it.isDone}.size
//             }
//             val output = futures.map{it.get()}.toString()
            
//             val endTime = LocalDateTime.now()
//             val timeDiff = ChronoUnit.MILLIS.between(startTime, endTime)
//             context.logger.log(Level.INFO,"Dummy_Single finished, time $timeDiff millis")
//             return request.createResponseBuilder(HttpStatus.OK).body("Finished, time $timeDiff millis\n\n$output").build()
//         }catch(e:Exception){
//             context.logger.log(Level.SEVERE,"Dummy_Single error",e)
//             return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body(e).build()
//         }
//     }
    
//     //@FunctionName("Dummy_Multi_Router")
//     fun Dummy_Multi_Router(
//         @HttpTrigger(name = "req", 
//         methods = [HttpMethod.GET], 
//             authLevel = AuthorizationLevel.ANONYMOUS)
//         request:HttpRequestMessage<Optional<String>>,
//         @DurableClientInput(name = "durableContext") durableContext:DurableClientContext,
//         context: ExecutionContext
//     ):HttpResponseMessage  {
//         try{
//             context.logger.log(Level.INFO,"Dummy_Multi_Router started")
//             val startTime = LocalDateTime.now()
//             val batchSize = request.queryParameters["batchSize"]?.toInt()?:1
//             val numberThreads = request.queryParameters["numberThreads"]?.toInt()?:10
//             val delayTime = request.queryParameters["delayTime"]?.toLong()?:0
//             val fileUrl = request.body.get()

//             val orchInput = DumOrchInput(batchSize, numberThreads, delayTime, fileUrl, startTime.toString())
            
//             val instanceId = durableContext.client.scheduleNewOrchestrationInstance("Dummy_Multi_Orchestrator",orchInput);

//             val endTime = LocalDateTime.now()
//             val timeDiff = ChronoUnit.MILLIS.between(startTime, endTime)
//             context.logger.log(Level.INFO,"Dummy_Multi_Router finished, time $timeDiff millis")
//             return durableContext.createCheckStatusResponse(request, instanceId);
//         }catch(e:Exception){
//             context.logger.log(Level.SEVERE,"Dummy_Multi_Router error",e)
//             return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body(e).build()
//         }
//     }
    
//     @FunctionName("Dummy_Multi_Orchestrator")
//     fun Dummy_Multi_Orchestrator(
//         @DurableOrchestrationTrigger(name = "taskOrchestrationContext") taskContext:TaskOrchestrationContext,
//         functionContext:ExecutionContext
//     ):DumOrchOutput  {
//         try{
//             orchlog(Level.INFO,"Dummy_Multi_Orchestrator started", taskContext=taskContext, functionContext=functionContext)
//             val input = taskContext.getInput(DumOrchInput::class.java)
//             //functionContext.logger.info("HERE 0")
//             try{
//                 val splitOutput = taskContext.callActivity("Dummy_Multi_Splitter", DumSplitInput(input), DumSplitOutput::class.java).await()
//                 //functionContext.logger.info("HERE 1")
            
//                 val parallelTasks:MutableList<Task<DumActivityOutput>> = mutableListOf()
//                 for(batchKey in splitOutput.batchKeyList){
//                     parallelTasks.add(taskContext.callActivity("Dummy_Multi_Activity", DumActivityInput(batchKey, input), DumActivityOutput::class.java))
//                 }
//                 //functionContext.logger.info("HERE 2")
            
//                 orchlog(Level.INFO,"Dummy_Multi_Orchestrator waiting for tasks", taskContext=taskContext, functionContext=functionContext)
                
//                 val subOutputs = taskContext.allOf(parallelTasks).await()
//                 //functionContext.logger.info("HERE 3")
//                 val out = subOutputs.map{it.outputMessage}.toString()
//                 //functionContext.logger.info("HERE 4")
            
//                 val startTime = LocalDateTime.parse(input.startTime)
//                 //functionContext.logger.info("HERE 5")
            
//                 val endTime = LocalDateTime.now()
//                 //functionContext.logger.info("HERE 6")
//                 val timeDiff = ChronoUnit.MILLIS.between(startTime, endTime)
//                 orchlog(Level.INFO,"Dummy_Multi_Orchestrator finished, time $timeDiff millis", taskContext=taskContext, functionContext=functionContext)
                
//                 return DumOrchOutput(timeDiff, splitOutput.timeDiffSplit, input, splitOutput, out, endTime.toString())
                
//             }catch(e:com.microsoft.durabletask.OrchestratorBlockedException){
//                 throw e
//             }catch(e:Exception){
//                 orchlog(Level.SEVERE,"Dummy_Multi_Orchestrator error ${e.localizedMessage}",e, taskContext=taskContext, functionContext=functionContext)
//                 return DumOrchOutput(null, null, input, null, "ERROR Dummy_Multi_Activity ${e.localizedMessage}", null)
//             }
//         }catch(e:Exception){
//             functionContext.logger.log(Level.SEVERE,"ERROR ${e.localizedMessage}",e)
//             throw e
//         }
//     }
    
//     @FunctionName("Dummy_Multi_Splitter")
//     fun Dummy_Multi_Splitter(
//         @DurableActivityTrigger(name = "input") input: DumSplitInput, 
//         context: ExecutionContext 
//     ):DumSplitOutput{
//         context.logger.log(Level.INFO,"Dummy_Multi_Splitter started ${input}")
//         val startTime = LocalDateTime.now()
//         val inputStream = commonOpenFileStream(input.orchInput.fileUrl)
//         val batchSize = input.orchInput.batchSize
//         val numberThreads = input.orchInput.numberThreads
        
//         val futures = mutableListOf<Future<String>>()
//         BufferedReader(InputStreamReader(inputStream)).use{
            
//             val headerLine = it.readLine()
//             val service = Executors.newFixedThreadPool(numberThreads)
            
//             var currentInBatch = 0
//             var stringBuilder = StringBuilder()
            
//             var nextLine = it.readLine()
//             while(nextLine != null) {
//                 currentInBatch++;
//                 stringBuilder.append("\n").append(nextLine);
//                 if(currentInBatch >= batchSize) {
//                     val batchSubmit = headerLine + stringBuilder.toString()
//                     futures.add(service.submit(Callable<String>{storeBatch(batchSubmit)}))
                    
//                     stringBuilder.clear()
//                     currentInBatch = 0
//                 }
//                 nextLine = it.readLine()
//             }
//             if(stringBuilder.isNotEmpty()){
//                 val batchSubmit = headerLine + stringBuilder.toString()
//                 futures.add(service.submit(Callable<String>{storeBatch(batchSubmit)}))
//             }
//         }
    
    
//         var stillRunning = futures.size
//         while(stillRunning > 0) {
//             stillRunning = futures.filter{!it.isDone}.size
//         }

//         val batchKeyList = futures.map{it.get()}
        
//         val endTime = LocalDateTime.now()
//         val timeDiff = ChronoUnit.MILLIS.between(startTime, endTime)

//         return DumSplitOutput(input, batchKeyList, timeDiff)
//     }
    
//     @FunctionName("Dummy_Multi_Activity")
//     fun Dummy_Multi_Activity(
//         @DurableActivityTrigger(name = "input") input: DumActivityInput, 
//         context: ExecutionContext 
//     ):DumActivityOutput{
//         try{
//             val content = retrieveBatch(input.batchKey)
//             val out=commonProcessBatch(content, input.orchInput.delayTime)
//             return DumActivityOutput(input, out)

//         }catch(e:Exception){
//             context.logger.log(Level.SEVERE,"ERROR",e)
//             throw e
//         }

//     }

//     private fun orchlog(
//         level:Level, msg:String, thrown:Throwable?=null, taskContext:TaskOrchestrationContext, functionContext:ExecutionContext
//     ){
//         var toLog = if (taskContext.getIsReplaying()) {"-REPLAY-"}else{""}
//         toLog = toLog + msg
//         if(thrown == null){
//             functionContext.logger.log(level, toLog);
//         } else{
//             functionContext.logger.log(level, toLog, thrown);
//         }
//     }

//     private fun storeBatch(content:String):String{

//         val connectionString = System.getenv("BlobConnection")
//         val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-SSSSSS"))
//         val rand = Random.nextInt(0,999)
//         val batchKey = "https://dexcsvdata001.blob.core.windows.net/processed/test-split/${timestamp}.R${rand}"
        
//         val client = BlobClientBuilder().connectionString(connectionString).endpoint(batchKey).buildClient()
//         client.blockBlobClient.upload(BinaryData.fromString(content))
//         return batchKey
//     }
    
//     private fun retrieveBatch(batchKey:String):String{

//         val connectionString = System.getenv("BlobConnection")
        
//         val client = BlobClientBuilder().connectionString(connectionString).endpoint(batchKey).buildClient()
//         return client.downloadContent().toString()
//     }
// }

// data class DumOrchInput(
//     @JsonProperty("batchSize")      val batchSize       : Int,
//     @JsonProperty("numberThreads")  val numberThreads   : Int,
//     @JsonProperty("delayTime")      val delayTime       : Long,
//     @JsonProperty("fileUrl")        val fileUrl         : String,
//     @JsonProperty("startTime")      val startTime       : String
// )

// data class DumOrchOutput(
//     @JsonProperty("timeDiff")       val timeDiff        : Long?,
//     @JsonProperty("timeDiffSplit")  val timeDiffSplit   : Long?,
//     @JsonProperty("input")          val input           : DumOrchInput,
//     @JsonProperty("splitOutput")    val splitOutput     : DumSplitOutput?,
//     @JsonProperty("outputMessage")  val outputMessage   : String,
//     @JsonProperty("endTime")        val endTime         : String?
// )

// data class DumSplitInput(
//     @JsonProperty("orchInput")      val orchInput       : DumOrchInput
// )

// data class DumSplitOutput(
//     @JsonProperty("input")          val input           : DumSplitInput,
//     @JsonProperty("batchKeyList")   val batchKeyList    : List<String>,
//     @JsonProperty("timeDiffSplit")  val timeDiffSplit   : Long
// )

// data class DumActivityInput(
//     @JsonProperty("batchKey")       val batchKey        : String,
//     @JsonProperty("orchInput")      val orchInput       : DumOrchInput
// )

// data class DumActivityOutput(
//     @JsonProperty("input")          val input           : DumActivityInput,
//     @JsonProperty("outputMessage")  val outputMessage   : String
// )