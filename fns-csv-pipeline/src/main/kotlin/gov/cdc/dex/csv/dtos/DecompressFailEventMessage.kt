package gov.cdc.dex.csv.dtos

import com.google.gson.annotations.SerializedName

data class DecompressFailEventMessage (
    @SerializedName("ingestMessage")    val ingestMessage : AzureBlobCreateEventMessage,
    @SerializedName("errorPath")        val errorPath : String,
    @SerializedName("failReason")       val failReason : String
)
