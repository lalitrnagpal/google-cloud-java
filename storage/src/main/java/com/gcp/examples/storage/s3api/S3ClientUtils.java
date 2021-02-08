package com.gcp.examples.storage.s3api;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Request;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.SignerFactory;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;

public class S3ClientUtils {
	
	public AmazonS3 getS3Client() throws IOException {

		String serviceAccountKey = "D://2-Workspace/my-service-account.json";

	    ServiceAccountCredentials serviceAccountCredentials;
	    
		serviceAccountCredentials = ServiceAccountCredentials.fromStream(new FileInputStream(serviceAccountKey));

		serviceAccountCredentials = (ServiceAccountCredentials) serviceAccountCredentials
	    									.createScoped(Arrays.asList("https://www.googleapis.com/auth/cloud-platform"));

	    final String projectId = serviceAccountCredentials.getProjectId();
	    GCPSessionCredentials gcpSessionCredentials = new GCPSessionCredentials((GoogleCredentials) serviceAccountCredentials);

	    SignerFactory.registerSigner("com.gcp.examples.storage.s3api.CustomGCPSigner", com.gcp.examples.storage.s3api.CustomGCPSigner.class);
	    ClientConfiguration clientConfig = new ClientConfiguration();
	    clientConfig.setSignerOverride("com.gcp.examples.storage.s3api.CustomGCPSigner");

	    AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
	        .withClientConfiguration(clientConfig)
	    	.withEndpointConfiguration(
	    		new AwsClientBuilder.EndpointConfiguration("https://storage.googleapis.com", "auto"))
	    					.withRequestHandlers(new RequestHandler2() {
	    						@Override
	    						public void beforeRequest(Request<?> request) {
	    							request.addHeader("x-goog-project-id", projectId);																		
	    						}				
	    					})
	    	.withCredentials(new AWSStaticCredentialsProvider(gcpSessionCredentials)).build();
	    
	    return s3Client;
		
	}
}
