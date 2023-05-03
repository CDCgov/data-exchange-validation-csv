package gov.cdc.dex.csv.dtos

import com.google.gson.annotations.SerializedName

data class DecompressOkEventMessage (
    @SerializedName("parent")           val parent : DecompressParentEventMessage,
    @SerializedName("processedPath")    val processedPath : String
)

data class DecompressFailEventMessage (
    @SerializedName("parent")           val parent : DecompressParentEventMessage,
    @SerializedName("errorPath")        val errorPath : String,
    @SerializedName("failReason")       val failReason : String
)

data class DecompressParentEventMessage (
    @SerializedName("ingestMessage")    val ingestMessage : AzureBlobCreateEventMessage,
    @SerializedName("parentMetadata")   val parentMetadata : Map<String,String>
)
