package org.smartpipe.aws.lambda;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;

import org.smartpipe.lib.MongoDbHelper;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;



public class Handler implements RequestHandler<S3EventNotification, Object> {
	
	public Object handleRequest(S3EventNotification input, Context context) {

		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_2).build();
		MongoClient mongoClient = null;
		try {
			mongoClient = new MongoClient(new MongoClientURI("mongodb://YOUR_EC2_INSTANCE_HOSTING_MONGO:27017"));
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		MongoDbHelper _dbHelper = new MongoDbHelper(mongoClient, "canon");
	
	    S3EventNotificationRecord record = input.getRecords().get(0);
	    String s3Key = record.getS3().getObject().getKey();
        String s3Bucket = record.getS3().getBucket().getName();
        context.getLogger().log("Bucket: " + s3Bucket+" File:"+s3Key);
        // retrieve s3 object
        S3Object object = s3Client.getObject(new GetObjectRequest(s3Bucket, s3Key));
        InputStream objectData = object.getObjectContent();

        System.out.println("Bucket: " + s3Bucket+" File:"+s3Key);
        try {
			_dbHelper.extractS3FileToLanding(s3Key, objectData);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("extractS3FileToCollection exception");
			e.printStackTrace();
		}
	    
	    return null;
	}
}
