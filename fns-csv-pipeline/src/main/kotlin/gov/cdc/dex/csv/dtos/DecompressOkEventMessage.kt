package gov.cdc.dex.csv.dtos

import com.google.gson.annotations.SerializedName

data class DecompressOkEventMessage (
    @SerializedName("ingestMessage")    val ingestMessage : AzureBlobCreateEventMessage,
    @SerializedName("processedPath")    val processedPath : String
)
