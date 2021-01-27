package com.gcp.examples.storage.googleapi;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.Lists;

public class UploadObjectGCPApi {

	public static void main(String[] args) {
		
	    // The ID of your GCP project
	    String projectId = "rosy-spring-280908";

	    // The ID of your GCS bucket
	    String bucketName = "java-test-bucket";

	    // The ID of your GCS object
	    String objectName = "100Records.csv";

	    // The path to your file to upload
	    String filePath = "100Records.csv";

	    GoogleCredentials credentials;
		try {
			credentials = GoogleCredentials.fromStream(new FileInputStream("D://2-Workspace/my-service-account.json"))
			        .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
	    
		    Storage storage = StorageOptions.newBuilder().setCredentials(credentials).setProjectId(projectId).build().getService();
		    BlobId blobId = BlobId.of(bucketName, objectName);
		    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
				storage.create(blobInfo, Files.readAllBytes(Paths.get(filePath)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	    System.out.println(
	        "File " + filePath + " uploaded to bucket " + bucketName + " as " + objectName);		

	}

}
