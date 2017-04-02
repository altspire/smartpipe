package org.smartpipe.lib;

import java.util.UUID;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
		_collectionName = FilenameUtils.removeExtension(inputFile.getName());
		CSVReader reader = new CSVReader(new FileReader(inputFile), ',' , '"');
		
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
		System.out.println(this.getCollection().count());
		
	}
	
	public void extractFileToLanding(String filePath) throws IOException
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
	
	public void extractS3FileToLanding(String fileName, InputStream fileInputStream) throws IOException
	{
		_collectionName = "landing_" + FilenameUtils.removeExtension(fileName);
		CSVReader reader = new CSVReader(new InputStreamReader(fileInputStream), '|' , '"');
		
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
	
	public void runAllMasterSpecValidations(DBCollection masterSpecCollection)
	{
		DBCursor cursor = masterSpecCollection.find(new BasicDBObject().append("FieldLookupModelKeyId", new BasicDBObject().append("$ne", "NULL")));
		HashMap<String, DBObject> parentChildMappings = new HashMap<String, DBObject>(); 
		
		while (cursor.hasNext()) 
		{
			DBObject fkValidationRuleDoc = cursor.next();
			
			if(_mongoClient.getDB(_databaseName).collectionExists("current_" + fkValidationRuleDoc.get("ModelKey")))
			{
				parentChildMappings.put(fkValidationRuleDoc.get("ModelKey") + "" + fkValidationRuleDoc.get("FieldKey") + fkValidationRuleDoc.get("FieldLookupModel") + fkValidationRuleDoc.get("FieldLookupField"), fkValidationRuleDoc);
			}
		}
		
		for(Map.Entry<String, DBObject> entry : parentChildMappings.entrySet()) 
		{
			System.out.println(entry.getValue().get("ModelKey") + " " + entry.getValue().get("FieldKey") + " " + entry.getValue().get("FieldLookupModel") + " " + entry.getValue().get("FieldLookupField"));
			
			doValidationsForCollection(_mongoClient.getDB(_databaseName).getCollection("current_" + entry.getValue().get("ModelKey")),
									   _mongoClient.getDB(_databaseName).getCollection("current_" + entry.getValue().get("FieldLookupModel")),
									   entry.getValue().get("FieldKey").toString(),
									   entry.getValue().get("FieldLookupField").toString());
		}
	}
	
	public void doValidationsForCollection(DBCollection childCollection, DBCollection parentCollection, String fieldKey, String fieldLookupField)
	{
		DBCursor cursor = childCollection.find();
		
		while (cursor.hasNext()) 
		{
			DBObject childDoc = cursor.next();
			
			if(parentCollection.findOne(new BasicDBObject().append(fieldLookupField, childDoc.get(fieldKey))) == null)
			{
				childCollection.update(new BasicDBObject().append("_id", childDoc.get("_id")), new BasicDBObject().append("$set", new BasicDBObject().append(fieldKey+"_fk", "0")));
			}
			else
			{
				childCollection.update(new BasicDBObject().append("_id", childDoc.get("_id")), new BasicDBObject().append("$set", new BasicDBObject().append(fieldKey+"_fk", "1")));
			}
		}
	}
	
	@SuppressWarnings("unused")
	private void addFieldWithValueToDoc(String DBName, String collName, String docID, String key, String value) {
	    BasicDBObject setNewFieldQuery = new BasicDBObject().append("$set", new BasicDBObject().append(key, value));
	    _mongoClient.getDB(DBName).getCollection(collName).update(new BasicDBObject().append("_id", docID), setNewFieldQuery);
	}

}
