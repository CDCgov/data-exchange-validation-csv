package gov.cdc.dex.csv.dtos

import com.google.gson.annotations.SerializedName

//NOTE: there are more fields in the message, but these are the only ones we care about
data class AzureBlobCreateEventMessage (
    @SerializedName("eventType")    val eventType : String?,
    @SerializedName("id")           val id        : String?,
    @SerializedName("data")         val evHubData : EvHubData?
)

data class EvHubData (
    @SerializedName("contentType")  val contentType       : String?,
    @SerializedName("url")          val url       : String?
)