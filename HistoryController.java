/**
 * 
 */
package org.bgu.ise.ddb.history;

import java.util.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

/**
 * @author 
 *
 */
@RestController
@RequestMapping(value = "/history")
public class HistoryController extends ParentController{
	
	/**
	 * The function inserts to the system storage triple(s)(username, title, timestamp). 
	 * The timestamp - in ms since 1970
	 * Advice: better to insert the history into two structures( tables) in order to extract it fast one with the key - username, another with the key - title
	 * @param username
	 * @param title
	 * @param response
	 */
	@RequestMapping(value = "insert_to_history", method={RequestMethod.GET})
	public void insertToHistory (@RequestParam("username")    String username,
			@RequestParam("title")   String title,
			HttpServletResponse response){
		//System.out.println(username + " "+title);
		//:TODO your implementation
		
		try {
			MongoClient mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase db = mongoClient.getDatabase("Big_Data_project");
			MongoCollection history_by_user = db.getCollection("HistoryByUser");
			MongoCollection history_by_title = db.getCollection("HistoryByTitle");
			MongoCollection users = db.getCollection("Users");
			MongoCollection mediaitems = db.getCollection("MediaItems");

			
			
			Bson user_filter = Filters.and(
            	    Filters.eq("username", username));
            	
			
        	boolean userExists = users.find(user_filter).first() != null;
        	
        	Bson title_filter = Filters.and(
            	    Filters.eq("TITLE", title));
            	
        	boolean titleExists = mediaitems.find(title_filter).first() != null;

        	if (titleExists && userExists) {
        		Document doc = new Document();
    	        doc.append("username", username);
    	        doc.append("title", title);
    	        doc.append("timeStamp", System.currentTimeMillis());
    	        
    	        history_by_user.insertOne(doc); // this table has an index on the username field for easy extraction
    	        history_by_title.insertOne(doc); // this table has an index on the title field for easy extraction
    	        
    			HttpStatus status = HttpStatus.OK;
    		    response.setStatus(status.value());
        	}
        	else {
        		HttpStatus status = HttpStatus.CONFLICT;
    			response.setStatus(status.value());
        	}
			
			mongoClient.close();
		}
		catch(Exception e){
			HttpStatus status = HttpStatus.CONFLICT;
			response.setStatus(status.value());
			e.printStackTrace();
		}
	}
	
	
	
	/**
	 * The function retrieves  users' history
	 * The function return array of pairs <title,viewtime> sorted by VIEWTIME in descending order
	 * @param username
	 * @return
	 */
	@RequestMapping(value = "get_history_by_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByUser(@RequestParam("entity")    String username){
		
		HistoryPair[] ret = null;
		
		try {
			MongoClient mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase db = mongoClient.getDatabase("Big_Data_project");
			MongoCollection history_by_user = db.getCollection("HistoryByUser");
			

            Document query = new Document("username", username);
            Document sort = new Document("timeStamp", -1);

			MongoCursor<Document> cursor = history_by_user.find(query).sort(sort).iterator();
			
	        ArrayList<HistoryPair> arrayList = new ArrayList<>();
			
			while (cursor.hasNext()) {
				
				Document document = cursor.next();
				
	            String title = document.getString("title");
	            Date timestamp = new Date(document.getLong("timeStamp"));
	            
	            HistoryPair temp  = new HistoryPair(title, timestamp);
	            arrayList.add(temp);
	            
			}
			
			ret = arrayList.toArray(new HistoryPair[arrayList.size()]);
					
			mongoClient.close();
			
			}
		catch(Exception e) {
				e.printStackTrace();
			}
		
			return ret;

	}
	
	
	/**
	 * The function retrieves  items' history
	 * The function return array of pairs <username,viewtime> sorted by VIEWTIME in descending order
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_history_by_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByItems(@RequestParam("entity")    String title){
		
		
		HistoryPair[] ret = null;
		
		try {
			MongoClient mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase db = mongoClient.getDatabase("Big_Data_project");
			MongoCollection history_by_item = db.getCollection("HistoryByTitle");
			

            Document query = new Document("title", title);
            Document sort = new Document("timeStamp", -1);

			MongoCursor<Document> cursor = history_by_item.find(query).sort(sort).iterator();
			
	        ArrayList<HistoryPair> arrayList = new ArrayList<>();
			
			while (cursor.hasNext()) {
				
				Document document = cursor.next();
				
	            String user = document.getString("username");
	            Date timestamp = new Date(document.getLong("timeStamp"));
	            
	            HistoryPair temp  = new HistoryPair(user, timestamp);
	            arrayList.add(temp);
	            
			}
			
			ret = arrayList.toArray(new HistoryPair[arrayList.size()]);
					
			mongoClient.close();
			
			}
		catch(Exception e) {
				e.printStackTrace();
			}
		
			return ret;
	}
	
	/**
	 * The function retrieves all the  users that have viewed the given item
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_users_by_item",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  User[] getUsersByItem(@RequestParam("title") String title){
		
		User[] ret = null;
		
		try {
			
		
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		MongoDatabase db = mongoClient.getDatabase("Big_Data_project");
		MongoCollection history_by_item = db.getCollection("HistoryByTitle");
		MongoCollection users = db.getCollection("Users");


        Document query = new Document("title", title);		
		
        ArrayList<User> arrayList = new ArrayList<>();

		Set<String> set = new HashSet<String> (); 
		
		MongoCursor<Document> cursor = history_by_item.find(query).iterator();


		while (cursor.hasNext()) {
			
			Document document = cursor.next();
			
            String user = document.getString("username");
           
            set.add(user);
		}
		
		cursor = users.find().iterator();
		
		while (cursor.hasNext()) {
			
			Document document = cursor.next();
			
            String username = document.getString("username");
            if (set.contains(username)) {
            	String lastname = document.getString("lastName");
            	String firstname = document.getString("firstName");
            	
            	User temp = new User(username, firstname, lastname);
            	
            	arrayList.add(temp);
            	
            	set.remove(username);
            }
		}

		ret = arrayList.toArray(new User[arrayList.size()]);

		mongoClient.close();
		}
		catch(Exception e){
			e.printStackTrace();			
		}
		
		return ret;
	}
	
	
	
	/**
	 * The function calculates the similarity score using Jaccard similarity function:
	 *  sim(i,j) = |U(i) intersection U(j)|/|U(i) union U(j)|,
	 *  where U(i) is the set of usernames which exist in the history of the item i.
	 * @param title1
	 * @param title2
	 * @return
	 */
	@RequestMapping(value = "get_items_similarity",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	public double  getItemsSimilarity(@RequestParam("title1") String title1,
			@RequestParam("title2") String title2){
		double ret = 0.0;

		try {
		
			User[] user1 = getUsersByItem(title1);
			User[] user2 = getUsersByItem(title2);
			
			Set<String> set = new HashSet<String> (); 
	
			for (int i = 0; i< user1.length; i++) {
				for (int j = 0; j< user2.length; j++) {
					if (user1[i].getUsername().equals(user2[j].getUsername())) {
						set.add(user1[i].getUsername());
					}
				}
				
			}
			
			double nom = (double) set.size();
			
			ret = (double) (set.size() / (user1.length + user2.length -nom));
		}
		
		catch(Exception e) {
			e.printStackTrace();			

		}
		return ret;
		
	}
	

}
