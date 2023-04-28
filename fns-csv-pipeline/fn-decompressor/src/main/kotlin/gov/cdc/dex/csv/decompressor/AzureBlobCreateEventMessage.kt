package gov.cdc.dex.csv.decompressor

import com.google.gson.annotations.SerializedName

//NOTE: there are more fields in the message, but these are the only ones we care about
data class AzBlobCreateEventMessage (
    @SerializedName("eventType" ) var eventType : String,
    @SerializedName("id"        ) var id        : String,
    @SerializedName("data"      ) var evHubData : EvHubData,
    @SerializedName("eventTime" ) var eventTime : String
)

data class EvHubData (
    @SerializedName("url"       ) var url       : String
)