/**
 * 
 */
package org.bgu.ise.ddb.items;

import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.net.URL;
import java.net.HttpURLConnection;
import java.io.BufferedReader;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.MediaItems;
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
@RequestMapping(value = "/items")
public class ItemsController extends ParentController {
	
	
	
	/**
	 * The function copy all the items(title and production year) from the Oracle table MediaItems to the System storage.
	 * The Oracle table and data should be used from the previous assignment
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@RequestMapping(value = "fill_media_items", method={RequestMethod.GET})
	public void fillMediaItems(HttpServletResponse response) throws Exception{
		//System.out.println("was here");
		ResultSet rs = null;
		Statement st = null;
		Connection con = null;
		
		try {
			String connectionUrl = "jdbc:sqlserver://" + "132.72.64.124" + ":1433;databaseName=" + "rubencho" + ";user=" + "rubencho" + ";" +
	                "password=" + "kY*rr/U1" + ";encrypt=false;";

			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"); //connection to db
	        con = DriverManager.getConnection(connectionUrl);
	        st = con.createStatement();
	        rs = st.executeQuery("Select PROD_YEAR, TITLE FROM MEDIAITEMS");        
			
	        
	        MongoClient mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase db = mongoClient.getDatabase("Big_Data_project");
			MongoCollection mediaItems = db.getCollection("MediaItems");
			
			
	        while (rs.next()) {
	        	String title = rs.getString("TITLE");
	            int prod_year = rs.getInt("PROD_YEAR");
	        	
	            
	            Bson filter = Filters.and(Filters.eq("TITLE", title),Filters.eq("PROD_YEAR", prod_year)
	            	);
	            
            	boolean documentExists = mediaItems.find(filter).first() != null;
	        
	       
	        if (!documentExists) {
	        	Document doc = new Document();
	            doc.append("PROD_YEAR", prod_year);
	            doc.append("TITLE", title);
	            mediaItems.insertOne(doc);

	         }
	                
	        }
	        
			mongoClient.close();
	        con.commit();
			rs.close();
	        st.close();       // close connections
	        con.close();
			
			
		}
		catch(Exception e){
			HttpStatus status = HttpStatus.CONFLICT;
			response.setStatus(status.value());
			e.printStackTrace();

		}
		
		finally {
			HttpStatus status = HttpStatus.OK;
			response.setStatus(status.value());
			
			try{
                if (st!= null){
                    st.close();
                }
            }
            catch(Exception e){
                e.printStackTrace();
            }
            try{
                if (rs!= null){
                    rs.close();
                }
            }
            catch(Exception e){
                e.printStackTrace();
            }
            try{
                if (con!= null){
                    con.close();
                }
            }
            catch(Exception e){
                e.printStackTrace();
            }
		}
	}
	
	
	/**
	 * The function copy all the items from the remote file,
	 * the remote file have the same structure as the films file from the previous assignment.
	 * You can assume that the address protocol is http
	 * @throws IOException 
	 */
	@RequestMapping(value = "fill_media_items_from_url", method={RequestMethod.GET})
	public void fillMediaItemsFromUrl(@RequestParam("url")    String urladdress,
			HttpServletResponse response) throws IOException{
		//System.out.println(urladdress);
		
		try {
		URL url = new URL(urladdress); 
	    HttpURLConnection con = (HttpURLConnection) url.openConnection();
	    con.setRequestMethod("GET");

        MongoClient mongoClient = new MongoClient("localhost", 27017);
		MongoDatabase db = mongoClient.getDatabase("Big_Data_project");
		MongoCollection mediaItems = db.getCollection("MediaItems");
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String line;
	    //Scanner scanner = new Scanner(con.getInputStream()); // Create a Scanner to read the data from the input stream
        reader.readLine();
    	while ((line = reader.readLine()) != null) {
    	    String [] values = line.split(",");
    	    
    	    
    	    Bson filter = Filters.and(
            	    Filters.eq("TITLE", values[0]),
            	    Filters.eq("PROD_YEAR", Integer.parseInt(values[1]))
            	);
            
        	boolean documentExists = mediaItems.find(filter).first() != null;
    	    
        	 if (!documentExists) {
        		 Document doc = new Document();
                 doc.append("PROD_YEAR", Integer.parseInt(values[1]));
                 doc.append("TITLE", values[0]);
                 mediaItems.insertOne(doc);

 	         }
    	    
    	}
	
		mongoClient.close();
    	reader.close();

		}
		
		catch(Exception e) {
			HttpStatus status = HttpStatus.CONFLICT;
			response.setStatus(status.value());
			e.printStackTrace();
		}
		
		finally {
			HttpStatus status = HttpStatus.OK;
			response.setStatus(status.value());
		}
		
	}
	
	/**
	 * The function retrieves from the system storage N items,
	 * order is not important( any N items) 
	 * @param topN - how many items to retrieve
	 * @return
	 */
	@RequestMapping(value = "get_topn_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(MediaItems.class)
	public  MediaItems[] getTopNItems(@RequestParam("topn")    int topN){
		//:TODO your implementation
		MediaItems[] ret = null;

		try {
		
		ret = new MediaItems[topN];
		
		
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		MongoDatabase db = mongoClient.getDatabase("Big_Data_project");
		MongoCollection mediaItems = db.getCollection("MediaItems");
		
		int index = 0;

		MongoCursor<Document> cursor = mediaItems.find().iterator();
		
		while (cursor.hasNext() && index<topN) {
			
			Document document = cursor.next();
			
            String title = document.getString("TITLE");
            int prod_year = document.getInteger("PROD_YEAR");
            
            MediaItems temp  = new MediaItems(title, prod_year);
            ret[index] = temp;
            index++;
            
			}
		
		mongoClient.close();
		
	    }
		catch(Exception e) {
			e.printStackTrace();
		}
		
			return ret;
		
		
		
		
		//MediaItems m = new MediaItems("Game of Thrones", 2011);
		//System.out.println(m);
		//return new MediaItems[]{m};
	}
		

}
