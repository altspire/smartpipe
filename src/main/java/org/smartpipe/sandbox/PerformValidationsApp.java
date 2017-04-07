package org.smartpipe.sandbox;

import java.io.IOException;

import org.smartpipe.lib.MongoDbHelper;

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class PerformValidationsApp {

	public static void main(String[] args) throws IOException 
	{
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
        MongoDbHelper _dbHelper = new MongoDbHelper(mongoClient, "canon");
        
        DBCollection masterSpecCollection = mongoClient.getDB("canon").getCollection("MasterSpecCombined");
        _dbHelper.runAllMasterSpecValidations(masterSpecCollection);
        _dbHelper.extractCurrentCollectionsToCSV();

	}

}
