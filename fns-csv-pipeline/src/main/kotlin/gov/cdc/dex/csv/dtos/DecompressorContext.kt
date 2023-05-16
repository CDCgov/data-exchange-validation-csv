package gov.cdc.dex.csv.dtos

import gov.cdc.dex.csv.services.EventService
import gov.cdc.dex.csv.services.BlobService

data class DecompressorContext (
    val blobService             : BlobService, 
    val eventService            : EventService,
    val connectionNames         : ConnectionNames,
    val requiredMetadataFields  : String
)