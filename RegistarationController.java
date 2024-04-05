/**
 * 
 */
package org.bgu.ise.ddb.registration;


import com.mongodb.*;
import java.io.IOException;
import javax.servlet.http.HttpServletResponse;


import java.util.Arrays;
import org.bson.Document;
import org.bson.types.ObjectId;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import java.util.Date;
import java.util.Calendar;
import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author
 *
 */
@RestController
@RequestMapping(value = "/registration")
public class RegistarationController extends ParentController{
	/**
	 * The function checks if the username exist,
	 * in case of positive answer HttpStatus in HttpServletResponse should be set to HttpStatus.CONFLICT,
	 * else insert the user to the system  and set to HttpStatus in HttpServletResponse HttpStatus.OK
	 * @param username
	 * @param password
	 * @param firstName
	 * @param lastName
	 * @param response
	 */
	@RequestMapping(value = "register_new_customer", method={RequestMethod.POST})
	public void registerNewUser(@RequestParam("username") String username,
			@RequestParam("password")    String password,
			@RequestParam("firstName")   String firstName,
			@RequestParam("lastName")  String lastName,
			HttpServletResponse response) throws Exception {
		System.out.println(username+" "+password+" "+lastName+" "+firstName);
		
		try {
			
			if (this.isExistUser(username)) {
				HttpStatus status = HttpStatus.CONFLICT;
				response.setStatus(status.value());
				return;
				}

			else {
				try {
					MongoClient mongoClient = new MongoClient("localhost", 27017);
					MongoDatabase db = mongoClient.getDatabase("Big_Data_project");
					MongoCollection users = db.getCollection("Users");
					
					Document doc = new Document();
                    doc.append("username", username);
                    doc.append("password", password);
                    doc.append("firstName", firstName);
                    doc.append("lastName", lastName);
                    doc.append("dateJoined", new Date());
                    users.insertOne(doc);
                    
    				HttpStatus status = HttpStatus.OK;
    			    response.setStatus(status.value());
    			    
    				mongoClient.close();
    				return;
	                }
				
				catch (MongoException me) {
					HttpStatus status = HttpStatus.CONFLICT;
					response.setStatus(status.value());
	                System.err.println("Unable to insert due to an error: " + me);
	                
	            }
				
			}
		}
				
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	
	/**
	 * The function returns true if the received username exist in the system otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "is_exist_user", method={RequestMethod.GET})
	public boolean isExistUser(@RequestParam("username") String username) throws IOException{
		System.out.println(username);
		boolean result = false;
		
		try {
			MongoClient mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase db = mongoClient.getDatabase("Big_Data_project");
			MongoCollection users = db.getCollection("Users");
			
			BasicDBObject myQuery = new BasicDBObject();
			myQuery.put("username", username);
			//DBCursor cursor = users.find(myQuery);
			MongoCursor<Document> cursor = users.find(myQuery).iterator();
			
			if (cursor.hasNext()) {
				result = true;
				}
			else {	
				result = false;
			}
			mongoClient.close();

		} 
		
		catch (MongoException e) {
			e.printStackTrace();
		}
			
		return result;
		
	 }
	
	/**
	 * The function returns true if the received username and password match a system storage entry, otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "validate_user", method={RequestMethod.POST})
	public boolean validateUser(@RequestParam("username") String username,
			@RequestParam("password")    String password) throws IOException{
		System.out.println(username+" "+password);
		boolean result = false;
		try {
			MongoClient mongoClient = new MongoClient("localhost", 27017);
			DB db = mongoClient.getDB("Big_Data_project");
			DBCollection users = db.getCollection("Users");
			BasicDBObject myQuery = new BasicDBObject();
			myQuery.put("username", username);
			myQuery.put("password", password);
			DBCursor cursor = users.find(myQuery);
			
			if (cursor.count() != 0) {
				result = true;
			}
			mongoClient.close();
		}
		
		catch (MongoException e) {
			e.printStackTrace();
		}
		
		
		return result;
		
	}
	
	/**
	 * The function retrieves number of the registered users in the past n days
	 * @param days
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "get_number_of_registred_users", method={RequestMethod.GET})
	public int getNumberOfRegistredUsers(@RequestParam("days") int days) throws IOException{
		int result = 0;

		try {
		System.out.println(days+"");

		Calendar calndr = Calendar.getInstance();
		calndr.add(Calendar.DAY_OF_YEAR, -days);
		Date n_days_ago = calndr.getTime();
		
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		MongoDatabase db = mongoClient.getDatabase("Big_Data_project");
		MongoCollection users = db.getCollection("Users");
		
		BasicDBObject myQuery = new BasicDBObject();
		myQuery.put("dateJoined", new BasicDBObject("$gte", n_days_ago));
		
		MongoCursor<Document> cursor = users.find(myQuery).iterator();
		while (cursor.hasNext()) {
			result++;
			cursor.next();
			}
		
		mongoClient.close();

		} 
		
		catch (Exception e){
			e.printStackTrace();
		}
			
		return result;
		
	}
	
	/**
	 * The function retrieves all the users
	 * @return
	 */
	@RequestMapping(value = "get_all_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(User.class)
	public  User[] getAllUsers(){
		User[] ret = null;
		
		try {
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		MongoDatabase db = mongoClient.getDatabase("Big_Data_project");
		MongoCollection users = db.getCollection("Users");
		
		ret = new User[(int)users.countDocuments()];
		int count = 0;

		MongoCursor<Document> cursor = users.find().iterator();
		
		
		while (cursor.hasNext()) {
			
			Document document = cursor.next();
			
            String username = document.getString("username");
            String firstname = document.getString("firstName");
            String lastname = document.getString("lastName");
            
            User temp  = new User(username, firstname, lastname);
            //System.out.println(temp);
            ret[count] = temp;
            count++;
            
			}
		
		mongoClient.close();
		
		}
		catch(Exception e) {
			e.printStackTrace();

		}
		
		return ret;
		
	}
	}
