package org.smartpipe.sandbox;

import java.io.IOException;

import org.smartpipe.lib.MongoDbHelper;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class LoadMasterSpecApp {

    static void usage() {
        System.err.println("usage: java LoadMasterSpecApp filepath");
        System.exit(-1);
    }
    
	public static void main(String[] args) throws IOException {
        if (args.length == 0 || args.length > 1)
            usage();
 
        // register directory and process its events
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
        MongoDbHelper _dbHelper = new MongoDbHelper(mongoClient, "canon");
        _dbHelper.extractFileToCollection(args[0]);
	}

}
