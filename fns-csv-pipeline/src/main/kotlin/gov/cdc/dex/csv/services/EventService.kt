package gov.cdc.dex.csv.services

interface EventService {
    fun sendOne(hubName: String, message: String ){sendBatch(hubName, listOf(message))}
    fun sendBatch(hubName: String, messages: List<String> )
}