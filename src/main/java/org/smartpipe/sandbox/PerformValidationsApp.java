package org.smartpipe.sandbox;

import java.net.UnknownHostException;

import org.smartpipe.lib.MongoDbHelper;

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class PerformValidationsApp {

	public static void main(String[] args) throws UnknownHostException 
	{
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
        MongoDbHelper _dbHelper = new MongoDbHelper(mongoClient, "canon");

//        DBCollection childCollection = mongoClient.getDB("canon").getCollection("current_PatientDemographicsClinicalDataRepository");
//        DBCollection parentCollection = mongoClient.getDB("canon").getCollection("current_PractitionerRosterClinicalDataRepository");
        
//        _dbHelper.doValidationsForCollection(childCollection, parentCollection, "PCPID", "PractitionerID");
        
        DBCollection masterSpecCollection = mongoClient.getDB("canon").getCollection("MasterSpecCombined");
        _dbHelper.runAllMasterSpecValidations(masterSpecCollection);

	}

}
