package com.gcp.examples.storage.s3api;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class UploadObjectS3Api {

	public static void main(String[] args) {
		
	    String bucketName = "java-test-bucket";

		try {

			AmazonS3 s3Client = new S3ClientUtils().getS3Client();
    	    
    	    s3Client.putObject(bucketName, "100Records.csv", "100Records.csv");
    	    
    	    S3Object s3Object = s3Client.getObject(bucketName, "100Records.csv");
    	    
    	    System.out.println("The following object was uploaded -> "+s3Object.getBucketName() + " : " + s3Object.getKey() + " : " + s3Object.getObjectMetadata().getLastModified());
    	    
    		if (s3Client.doesBucketExistV2(bucketName)) {
    			System.out.println("This bucket "+ bucketName + " exists");
    		}
    	    
    		System.out.println("Listing all the buckets now.");
    		List<Bucket> buckets = s3Client.listBuckets(); 
    		for (Bucket bucket : buckets) { 
    		   System.out.println(bucket.getName()); 
    		}
    		
    		System.out.println("Creating a Bucket -> " + bucketName + "-121");
    		s3Client.createBucket(new CreateBucketRequest(bucketName + "-121", "us-east1"));
    		
    		System.out.println("Listing all the buckets now.");
//    		buckets = s3Client.listBuckets(); 
//    		for (Bucket bucket : buckets) { 
//    		   System.out.println(bucket.getName()); 
//    		}
    		
    		System.out.println("Deleting a Bucket -> " + bucketName + "-121");
    		s3Client.deleteBucket(bucketName + "-121");
    		
    		System.out.println("Listing all the buckets now.");
//    		buckets = s3Client.listBuckets(); 
//    		for (Bucket bucket : buckets) { 
//    		   System.out.println(bucket.getName()); 
//    		}
    		
    		// Downloading an Object
    		
    		System.out.println("Downloading " + bucketName + " as " + "100Records-dup.csv");
    		S3Object s3object = s3Client.getObject(bucketName, "100Records.csv"); 
    		S3ObjectInputStream inputStream = s3object.getObjectContent(); 
    		FileUtils.copyInputStreamToFile(inputStream, new File("D://2-Workspace/Downloaded-100Records.csv"));

    		// Copying an Object 
    		System.out.println("Copying 100Records.csv as 100Records-dup.csv");
    		s3Client.copyObject( bucketName, "100Records.csv", bucketName, "100Records-dup.csv" );
    		
    		System.out.println("Deleting duplicate object " + bucketName + " as " + "100Records-dup.csv");
    		s3Client.deleteObject(bucketName, "100Records-dup.csv");
    		
    		System.out.println("Listing all the buckets now.");
//    		buckets = s3Client.listBuckets(); 
//    		for (Bucket bucket : buckets) { 
//    		   System.out.println(bucket.getName()); 
//    		}
    		
		} catch (IOException e) {
			e.printStackTrace();
		}

/*
 * 
		## Creating a Bucket
		
		String bucketName = "baeldung-bucket"; 
		if (s3Client.doesBucketExist(bucketName)) { 
		   LOG.info("Bucket name is not available." + " Try again with a different Bucket name."); 
		   return; 
		} 
		
		s3Client.createBucket(bucketName);
		
		## Listing Buckets
		
		List<Bucket> buckets = s3Client.listBuckets(); 
		for (Bucket bucket : buckets) { 
		   System.out.println(bucket.getName()); 
		}
		
		## Deleting a Bucket
		
		try { 
		   s3Client.deleteBucket("baeldung-bucket-test2"); 
		} catch (AmazonServiceException e) { 
		   System.err.println("e.getErrorMessage()); return; 
		}
		
		## Uploading Objects
		
		s3Client.putObject( bucketName, "Document/hello.txt", new File("/Users/user/Document/hello.txt") );
		
		Listing Objects
		
		ObjectListing objectListing = s3Client.listObjects(bucketName); 
		for(S3ObjectSummary os : objectListing.getObjectSummaries()) { 
		   LOG.info(os.getKey()); 
		}
		
		## Downloading an Object
		
		S3Object s3object = s3Client.getObject(bucketName, "picture/pic.png"); 
		S3ObjectInputStream inputStream = s3object.getObjectContent(); 
		FileUtils.copyInputStreamToFile(inputStream, new File("/Users/user/Desktop/hello.txt"));
		
		## Copying, Renaming and Moving an Object
		
		s3Client.copyObject( "baeldung-bucket", "picture/pic.png", "baeldung-bucket2", "document/picture.png" );
		
		We can copy an object by calling copyObject() method on our s3Client which accepts four parameters:
		1.	source bucket name
		2.	object key in source bucket
		3.	destination bucket name (it can be same as source)
		4.	object key in destination bucket
		
		## Deleting an Object
		
		s3Client.deleteObject("baeldung-bucket","picture/pic.png");
		
		## Deleting Multiple Objects
		
		String objkeyArr[] = { "document/hello.txt", "document/pic.png" }; 
		DeleteObjectsRequest delObjReq = new DeleteObjectsRequest("baeldung-bucket")
		                                           .withKeys(objkeyArr); 
		s3Client.deleteObjects(delObjReq);

 * 	    	    
 */
	    	    
	}

}
