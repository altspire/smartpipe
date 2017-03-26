package org.smartpipe.lib;

import java.util.UUID;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;


import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.opencsv.CSVReader;


public class MongoDbHelper {
	
	private String _databaseName;
	private String _collectionName;
	private String[] _attributeNames;
	private MongoClient _mongoClient;
	private UUID _lineageId;
	
	public MongoDbHelper(MongoClient mongoClient, String databaseName)
	{
		_mongoClient = mongoClient;
		_databaseName = databaseName;
		_lineageId = UUID.randomUUID();
	}
	
	public DBCollection getCollection()
	{
		return _mongoClient.getDB(_databaseName).getCollection(_collectionName);
	}
	
	public void insertDocument(String[] values)
	{
		this.getCollection().insert(this.toDBObject(values));
	}
	
	private DBObject toDBObject(String[] values)
	{
		BasicDBObject newObject = new BasicDBObject();
		
		newObject.append("lineage_id", _lineageId.toString());
		newObject.append("date_created", System.nanoTime());
		
		for(int i=0;i<values.length;i++)
		{
			newObject.append(_attributeNames[i], values[i]);
		}
		
		return newObject;
	}
	
	public void extractFileToCollection(String filePath) throws IOException
	{
		File inputFile = new File(filePath);
		_collectionName = "landing_" + FilenameUtils.removeExtension(inputFile.getName());
		CSVReader reader = new CSVReader(new FileReader(inputFile), '|' , '"');
		try 
		{
		  //Read CSV line by line and use the string array as you want
		  String[] nextLine;
		  _attributeNames = reader.readNext();
		
			while ((nextLine = reader.readNext()) != null) {
			     if (nextLine != null) {
			        //Verifying the read data here
			    	 this.insertDocument(nextLine);
			     }
			}

		}
		finally
		{
			reader.close();
		}
		doLandingToHistoryForCollection(this.getCollection());
		
		System.out.println(this.getCollection().count());
		
	}
	
	public void doLandingToHistory()
	{
		Set<String> colls = _mongoClient.getDB(_databaseName).getCollectionNames();

		for (String s : colls) {
			if(s.contains("landing_")){
				doLandingToHistoryForCollection(_mongoClient.getDB(_databaseName).getCollection(s));
			}
		}

	}
	
	private HashSet<String> generateIDFieldsQuery(DBCollection collection)
	{
		HashSet<String> IDFields = new HashSet<String>();
		for (String attributeName: _attributeNames)
		{
			if(attributeName.toLowerCase().endsWith("id"))
			{
				IDFields.add(attributeName);
			}
		}
		return IDFields;
	}
	
	private void doLandingToHistoryForCollection(DBCollection collection)
	{
		DBCollection historyCollection =_mongoClient.getDB(_databaseName).getCollection("history_" + collection.getName().replace("landing_", ""));	
		DBCursor cursor = collection.find(new BasicDBObject().append("lineage_id", this._lineageId.toString()));
		HashSet<String> IDFields = generateIDFieldsQuery(historyCollection);
		
		while (cursor.hasNext()) {
			DBObject landingDoc = cursor.next();
			landingDoc.removeField("_id");
			
			BasicDBObject IDFieldQuery = new BasicDBObject();
			for(String IDField: IDFields)
			{
				IDFieldQuery.append(IDField, landingDoc.get(IDField));
			}

			DBCursor historyCollectionCursor = historyCollection.find(IDFieldQuery).sort(new BasicDBObject("_id", -1));
			DBObject historyDoc = null;
			
			if(historyCollectionCursor.hasNext())
			{
				historyDoc = historyCollectionCursor.next();
			}
			
			if(historyDoc != null) {
				historyDoc.removeField("_id");
				
				for(String key: historyDoc.keySet()){
					if(!key.equals("lineage_id") && !key.equals("date_created") && (historyDoc.keySet().size() != landingDoc.keySet().size() || !landingDoc.get(key).equals(historyDoc.get(key)))){
						historyCollection.insert(landingDoc);				
						break;
					}
				}
			} else {
				historyCollection.insert(landingDoc);				
			}

		}
		doHistoryToCurrentForCollection(historyCollection);
			
	}
	
	private void doHistoryToCurrentForCollection(DBCollection collection)
	{
		DBCollection currentCollection =_mongoClient.getDB(_databaseName).getCollection("current_" + collection.getName().replace("history_", ""));	
		DBCursor cursor = collection.find(new BasicDBObject().append("lineage_id", this._lineageId.toString()));
		HashSet<String> IDFields = generateIDFieldsQuery(currentCollection);
		
		while (cursor.hasNext()) {
			DBObject historyDoc = cursor.next();
			historyDoc.removeField("_id");

			BasicDBObject IDFieldQuery = new BasicDBObject();
			for(String IDField: IDFields)
			{
				IDFieldQuery.append(IDField, historyDoc.get(IDField));
			}

			DBObject currentDoc = currentCollection.findOne(IDFieldQuery);
			
			if(currentDoc != null) {
				currentDoc.removeField("_id");
				
				for(String key: currentDoc.keySet()){
					if(!key.equals("lineage_id") && !key.equals("date_created") && (currentDoc.keySet().size() != historyDoc.keySet().size() || !currentDoc.get(key).equals(historyDoc.get(key)))){
						currentCollection.update(IDFieldQuery, historyDoc, true, false);
						continue;
					}
				}
			} else {
				currentCollection.insert(historyDoc);				
			}

		}
			
	}

}
