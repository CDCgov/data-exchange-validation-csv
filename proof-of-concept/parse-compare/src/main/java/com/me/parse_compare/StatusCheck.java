package com.me.parse_compare;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StatusCheck {
	private static final ObjectMapper MAPPER = new ObjectMapper();

	public static void main(String[] args) throws StreamReadException, DatabindException, IOException {
		
		List<StatusCheckInput> inputs = Arrays.asList(
				MAPPER.readValue(new File("C:\\Users\\jacob.r.peterson\\Downloads\\2023-07-25-status-check-small.json"), StatusCheckInput[].class));
		
		
		System.out.println("batch size,threads,delay,file,multi?,time,timeSplit,error");
		
		for(StatusCheckInput input:inputs) {
			String statusUrl = input.getStatusLinks().getStatusQueryGetUri();
			HttpURLConnection con = (HttpURLConnection) new URL(statusUrl).openConnection();
			con.setRequestMethod("GET");
			String response;
			try(BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()))){
				String inputLine;
				StringBuffer content = new StringBuffer();
				while ((inputLine = in.readLine()) != null) {
				    content.append(inputLine);
				}
				response = content.toString();
			}finally {
				con.disconnect();
			}
			
			String outLine = input.getOrchInput().getBatchSize()+","
							+input.getOrchInput().getNumberThreads()+","
							+input.getOrchInput().getDelayTime()+","
							+input.getOrchInput().getFileUrl()+","
							+input.getsOrM()+",";
			
			StatusOutput output = MAPPER.readValue(response, StatusOutput.class);
			
			if("Failed".equals(output.getRuntimeStatus())) {
				outLine += ",,"+output.getOutput().getMessage()+":"+output.getOutput().getDetails().replace("\n", "|||").replace("\r", "|||");
			}else if("Completed".equals(output.getRuntimeStatus())) {
				outLine += output.getOutput().getTimeDiff()+","+output.getOutput().getTimeDiffSplit()+","+output.getOutput().getErrorMessage();
			}else {
				throw new RuntimeException("NOT DONE YET");
			}
			
			System.out.println(outLine);
		}
		
		// TODO Auto-generated method stub

	}
	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class StatusOutput{
		private String runtimeStatus;
		private Output output;
		public String getRuntimeStatus() {
			return runtimeStatus;
		}
		public void setRuntimeStatus(String runtimeStatus) {
			this.runtimeStatus = runtimeStatus;
		}
		public Output getOutput() {
			return output;
		}
		public void setOutput(Output output) {
			this.output = output;
		}
		
		
	}
	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class Output{
		private String message,details, errorMessage;
		private Integer timeDiff,timeDiffSplit;
		public String getMessage() {
			return message;
		}
		public void setMessage(String message) {
			this.message = message;
		}public String getDetails() {
			return details;
		}public void setDetails(String details) {
			this.details = details;
		}
		public String getErrorMessage() {
			return errorMessage;
		}
		public void setErrorMessage(String errorMessage) {
			this.errorMessage = errorMessage;
		}
		public Integer getTimeDiff() {
			return timeDiff;
		}
		public void setTimeDiff(Integer timeDiff) {
			this.timeDiff = timeDiff;
		}
		public Integer getTimeDiffSplit() {
			return timeDiffSplit;
		}
		public void setTimeDiffSplit(Integer timeDiffSplit) {
			this.timeDiffSplit = timeDiffSplit;
		}
		
		
	}
////////////////////////////////////////////////////////////////////////////////////////////
	public static class StatusCheckInput{
		private StatusLinks statusLinks;
		private String sOrM;
		private OrchInput orchInput;
		public StatusLinks getStatusLinks() {
			return statusLinks;
		}
		public void setStatusLinks(StatusLinks statusLinks) {
			this.statusLinks = statusLinks;
		}
		public String getsOrM() {
			return sOrM;
		}
		public void setsOrM(String sOrM) {
			this.sOrM = sOrM;
		}
		public OrchInput getOrchInput() {
			return orchInput;
		}
		public void setOrchInput(OrchInput orchInput) {
			this.orchInput = orchInput;
		}
		
		
	}
	
	public static class StatusLinks{
		private String id,purgeHistoryDeleteUri,sendEventPostUri,statusQueryGetUri,terminatePostUri;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getPurgeHistoryDeleteUri() {
			return purgeHistoryDeleteUri;
		}

		public void setPurgeHistoryDeleteUri(String purgeHistoryDeleteUri) {
			this.purgeHistoryDeleteUri = purgeHistoryDeleteUri;
		}

		public String getSendEventPostUri() {
			return sendEventPostUri;
		}

		public void setSendEventPostUri(String sendEventPostUri) {
			this.sendEventPostUri = sendEventPostUri;
		}

		public String getStatusQueryGetUri() {
			return statusQueryGetUri;
		}

		public void setStatusQueryGetUri(String statusQueryGetUri) {
			this.statusQueryGetUri = statusQueryGetUri;
		}

		public String getTerminatePostUri() {
			return terminatePostUri;
		}

		public void setTerminatePostUri(String terminatePostUri) {
			this.terminatePostUri = terminatePostUri;
		}
		
		
	}
	
	public static class OrchInput{
		private int batchSize,numberThreads, delayTime;
		private String fileUrl, startTime;
		public int getBatchSize() {
			return batchSize;
		}
		public void setBatchSize(int batchSize) {
			this.batchSize = batchSize;
		}
		public int getNumberThreads() {
			return numberThreads;
		}
		public void setNumberThreads(int numberThreads) {
			this.numberThreads = numberThreads;
		}
		public int getDelayTime() {
			return delayTime;
		}
		public void setDelayTime(int delayTime) {
			this.delayTime = delayTime;
		}
		public String getFileUrl() {
			return fileUrl;
		}
		public void setFileUrl(String fileUrl) {
			this.fileUrl = fileUrl;
		}
		public String getStartTime() {
			return startTime;
		}
		public void setStartTime(String startTime) {
			this.startTime = startTime;
		}
		
		
	}
}
