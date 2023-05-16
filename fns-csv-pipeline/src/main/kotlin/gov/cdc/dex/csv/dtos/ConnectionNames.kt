package gov.cdc.dex.csv.dtos

data class ConnectionNames (
    val eventHubs   : EventHubNames,
    val blobStorage : BlobStorageNames,
)

data class EventHubNames (
    val ingest          : String,
    val decompressOk    : String,
    val decompressFail  : String
)

data class BlobStorageNames (
    val ingest      : String,
    val processed   : String,
    val error       : String
)