/**
 * 
 */
package org.bgu.ise.ddb.registration;



import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;  
import java.sql.Timestamp;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/**
 * @author Alex
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
			HttpServletResponse response){
		System.out.println(username+" "+password+" "+lastName+" "+firstName);
		//:TODO your implementation
		try {
			//check if the users exist
			if (isExistUser(username)) {
				System.out.println("the user is exist...");
				HttpStatus status = HttpStatus.CONFLICT;
				response.setStatus(status.value());
			}else {
				//make the connection to the mongoDb localhost database
				MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
				DBCollection  dbCollection =mongoClient.getDB("FProjectDB").getCollection("users");
				
				System.out.println("adding user to the db...");
				//Create new user object
				BasicDBObject register = new BasicDBObject();
				register.put("username", username);
				register.put("firstName", firstName);
				register.put("lastName", lastName);
				register.put("password", password);
				register.put("registerdate",  new Timestamp(System.currentTimeMillis()));
				
				//add the register to the user collection 
				dbCollection.insert(register);
				mongoClient.close();

				HttpStatus status = HttpStatus.OK;
				response.setStatus(status.value());
			}
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
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
		//:TODO your implementation
		MongoClient mongoClient = null;
		DBCollection  dbCollection=null;
		BasicDBObject queryResult = new BasicDBObject();
		try {
			//connect to mongoDb local host
			mongoClient = new MongoClient( "localhost" , 27017 );
			dbCollection = mongoClient.getDB("FProjectDB").getCollection("users");
			
			// get all the usernames the contain the specific username 
			queryResult.put("username", username);
			DBCursor dbCursor = dbCollection.find(queryResult);
			while (dbCursor.hasNext()) 
			{ 
				result = true;
				dbCursor.next();
			}
			mongoClient.close();
		} catch (Exception e) {
			System.out.println(e);
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
		//:TODO your implementation
		MongoClient mongoClient = null;
		try {
			//connect to localhost mongoDb 
			mongoClient = new MongoClient( "localhost" , 27017 );
			DBCollection  dbCollection = mongoClient.getDB("FProjectDB").getCollection("users");
			
			BasicDBObject queryResult = new BasicDBObject();
			//set the query and find match data
			queryResult.put("username", username);
			queryResult.put("password", password);
			DBCursor dbCursor = dbCollection.find(queryResult);
			
			//check if there user that have the spicfic username and password
			while (dbCursor.hasNext()) 
			{ 
				result = true;
				dbCursor.next();
			}
			mongoClient.close();
		}catch (Exception e) {
			System.out.println(e);
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
		System.out.println(days+"");
		int result = 0;
		//:TODO your implementation
		MongoClient mongoClient = null;
		try {
			//connect to localHost mongoDb and get users collection 
			mongoClient = new MongoClient( "localhost" , 27017 );
			DBCollection  dbCollection = mongoClient.getDB("FProjectDB").getCollection("users");
			DBCursor dbCursor = dbCollection.find();
			
			//calculate the start date 
			long millsec=days * 24 * 3600 * 1000;
			Date now= new Date();
			Timestamp startDate = new Timestamp( now.getTime() - millsec );

			while (dbCursor.hasNext()) 
			{ 
				//get for each user registerDate
				DBObject theObj = dbCursor.next();
				Date registerdate =  (Date)theObj.get("registerdate");
				
				//check and add to the result if the register date bigger than the start date
				if( registerdate.getTime()> startDate.getTime()) {
					result++;
				}
			}
			mongoClient.close();
		}catch (Exception e) {
			System.out.println(e);
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
		//:TODO your implementation
		ArrayList<User> users = new ArrayList<User>();
		MongoClient mongoClient = null;
		try {
			//connect to localHost mongoDb 
			mongoClient = new MongoClient( "localhost" , 27017 );
			DBCollection  dbCollection = mongoClient.getDB("FProjectDB").getCollection("users");

			DBCursor dbCursor = dbCollection.find();
			
			//get all the data foreach user and insert into user object and add the object to the arrayList
			while (dbCursor.hasNext()) 
			{ 
				DBObject theObj = dbCursor.next();
				String username = (String) theObj.get("username");
				String firstName = (String) theObj.get("firstName");
				String lastName = (String) theObj.get("lastName");
				String password = (String) theObj.get("password");
				users.add(new User(username, password ,firstName, lastName));
			}
			mongoClient.close();
		} catch (Exception e) {
			System.out.println(e);
		}
		
		User[] allUsers=users.toArray(new User[users.size()]);
		return allUsers;
	}

}
