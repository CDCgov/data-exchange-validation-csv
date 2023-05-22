package gov.cdc.dex.csv.services

import com.azure.messaging.eventhubs.EventData
import com.azure.messaging.eventhubs.EventHubClientBuilder

class AzureEventServiceImpl (val connectionStr: String) : EventService {
    
    override fun sendBatch(hubName: String, messages: List<String> ) {
        val producerClient = EventHubClientBuilder().connectionString(connectionStr,  hubName).buildProducerClient()
        producerClient.use{producer -> 
            var eventDataBatch = producer.createBatch()
            for(message in messages){
                val eventData = EventData(message)

                // try to add the event to the batch
                if (!eventDataBatch.tryAdd(eventData)) {
                    // if the batch is full, send it and then create a new batch
                    producer.send(eventDataBatch)
                    eventDataBatch = producer.createBatch()

                    // Try to add that event that couldn't fit before.
                    if (!eventDataBatch.tryAdd(eventData)) {
                        throw IllegalArgumentException("Event is too large for an empty batch. Max size: "
                                + eventDataBatch.maxSizeInBytes
                        )
                    }
                }
            }
            // send the last batch of remaining events
            if (eventDataBatch.count > 0) {
                producer.send(eventDataBatch)
            }
        }
    }
}