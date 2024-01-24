/**
 * 
 */
package org.bgu.ise.ddb.history;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

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
		System.out.println(username+" "+title);
		//:TODO your implementation
		HttpStatus status = null;
		MongoClient mongoClient =null;
		DBCollection  dbCollection=null;
		int i=0;
		try {
			mongoClient = new MongoClient( "localhost" , 27017 );
			dbCollection = mongoClient.getDB("FProjectDB").getCollection("History");
			i++;
			BasicDBObject userHistory = new BasicDBObject();
			userHistory.put("username", username);
			userHistory.put("Title", title);
			userHistory.put("Timestamp", (new Timestamp(System.currentTimeMillis())).getTime() );
			i++;
			if(isExistUser(username) &&isTittleExist(title))
			{
				dbCollection.insert(userHistory);
				status = HttpStatus.OK;
			}else {
				status = HttpStatus.CONFLICT;
			}
			i++;
			mongoClient.close();
		} catch (Exception e) {
			System.out.println(e);
		}

		response.setStatus(status.value());
	}
	
	private boolean isExistUser(String username ){
		boolean result = false;
		//:TODO your implementation
		MongoClient mongoClient = null;
		DBCollection  dbCollection=null;
		BasicDBObject queryResult = new BasicDBObject();
		int i=0;
		try {
			//connect to mongoDb local host
			mongoClient = new MongoClient( "localhost" , 27017 );
			dbCollection = mongoClient.getDB("FProjectDB").getCollection("users");
			i++;
			// get all the usernames the contain the specific username 
			queryResult.put("username", username);
			DBCursor dbCursor = dbCollection.find(queryResult);
			while (dbCursor.hasNext()) 
			{ 
				i++;
				//System.out.println(i);
				result = true;
				dbCursor.next();
			}
			mongoClient.close();
		} catch (Exception e) {
			System.out.println(e);
		}
		return result;
		
	}
	
	private boolean isTittleExist(String title) {
		boolean result = false;
		MongoClient mongoClient = null;
		DBCollection  dbCollection =null;
		BasicDBObject queryResult=null;
		int check=0;
		try {
			mongoClient = new MongoClient( "localhost" , 27017 );
			dbCollection = (DBCollection) mongoClient.getDB("FProjectDB").getCollection("MediaItems");
			queryResult = new BasicDBObject();
			queryResult.put("Title", title);
			check++;
			DBCursor dbCursor = dbCollection.find(queryResult);
			while (dbCursor.hasNext()) 
			{ 
				result = true;
				check++;
				dbCursor.next();
				//System.out.println(check);
			}
			mongoClient.close();
		} catch (Exception e) {
			System.out.println(e);
		}
		return result;
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
		//:TODO your implementation
		ArrayList<HistoryPair> historyPairs = new ArrayList<HistoryPair>();
		MongoClient mongoClient = null;
		DBCollection  dbCollection=null;
		BasicDBObject queryResult=null;
		try {
			int check=0;
			mongoClient = new MongoClient( "localhost" , 27017 );
			dbCollection = mongoClient.getDB("FProjectDB").getCollection("History");
			check++;
			queryResult = new BasicDBObject();
			queryResult.put("username", username);
			check++;
			//change sort
			DBCursor dbCursor = dbCollection.find(queryResult).sort(new BasicDBObject("Timestamp",-1));
			check++;
			while (dbCursor.hasNext()) 
			{ 
				DBObject theObj = dbCursor.next();
				String credentials = (String) theObj.get("Title");
				long timestamp = (long) theObj.get("Timestamp");
				historyPairs.add(new HistoryPair(credentials,new Date(timestamp)));

			}
			check++;
			//System.out.println(check);
			mongoClient.close();
		} catch (Exception e) {
			System.out.println(e);
		}
		
		HistoryPair[] HistoryByUser=historyPairs.toArray(new HistoryPair[historyPairs.size()]);
		return HistoryByUser;
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
		//:TODO your implementation
		ArrayList<HistoryPair> historyPairs = new ArrayList<HistoryPair>();
		MongoClient mongoClient = null;
		DBCollection  dbCollection=null;
		BasicDBObject queryResult=null;
		int check=0;
		try {
			mongoClient = new MongoClient( "localhost" , 27017 );
			dbCollection = mongoClient.getDB("FProjectDB").getCollection("History");
			queryResult = new BasicDBObject();
			queryResult.put("Title", title);
			check++;
			
			DBCursor dbCursor = dbCollection.find(queryResult).sort(new BasicDBObject("Timestamp",-1));
			
			while (dbCursor.hasNext()) 
			{ 
				DBObject theObj = dbCursor.next();
				String credentials = (String) theObj.get("username");
				check++;
				//Timestamp timestamp = theObj.get("Timestamp"); -> not work because i can cast DBobject to timestamp
				long timestamp = (long) theObj.get("Timestamp");
				historyPairs.add(new HistoryPair(credentials,new Date(timestamp)));
				//System.out.println(check);

			}
			mongoClient.close();
		} catch (Exception e) {
			System.out.println(e);
		}
		
		HistoryPair[] HistoryByItems=historyPairs.toArray(new HistoryPair[historyPairs.size()]);
		return HistoryByItems;
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
		//:TODO your implementation
		ArrayList<User> users = new ArrayList<User>();
		DBCollection  dbCollection=null;
		BasicDBObject queryResult=null;
		MongoClient mongoClient = null;
		
		try {
			HistoryPair[] historyPairs = getHistoryByItems(title);
			int check=0;
			mongoClient = new MongoClient( "localhost" , 27017 );
			dbCollection = mongoClient.getDB("projectDB").getCollection("Users");
			queryResult = new BasicDBObject();
			
			check++;
			for (int i=0;i<historyPairs.length;i++) {
				
				queryResult.put("username", historyPairs[i].credentials);
				DBCursor dbCursor = dbCollection.find(queryResult);
				
				if (dbCursor.hasNext()) 
				{ 
					DBObject theObj = dbCursor.next();
					String username = (String) theObj.get("username");
					String firstName = (String) theObj.get("firstName");
					String lastName = (String) theObj.get("lastName");
					users.add(new User(username, firstName, lastName));
					check++;
				}
				mongoClient.close();
			}
			//System.out.println(check);
		} catch (Exception e) {
			System.out.println(e);
		}
		
		User[] UsersByItem=users.toArray(new User[users.size()]);
		return UsersByItem;
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
		//:TODO your implementation
		double ret = 0.0;
		Set<String> userListTittle1=new HashSet<String>();
		Set<String> userListTittle2=new HashSet<String>();
		HistoryPair[] historyPairsTittle1;
		HistoryPair[] historyPairsTittle2;
		try {

			historyPairsTittle1=getHistoryByItems(title1);
			historyPairsTittle2=getHistoryByItems(title2);
			
			for (int i=0;i<historyPairsTittle1.length;i++) {
				userListTittle1.add(historyPairsTittle1[i].credentials);
			}	
			
			for (int i=0;i<historyPairsTittle2.length;i++) {
				userListTittle2.add(historyPairsTittle2[i].credentials);
			}	
			
			//String[] tempUserTittle2 = userListTittle2.toArray(new String[userListTittle2.size()]);
			//Set<String> union=  new HashSet<String>(userListTittle1);
			//for (int i=0;i<userListTittle2.size();i++) {
			//	union.add(tempUserTittle2[i]);
			//}	
			
			Set<String> union=  new HashSet<String>(userListTittle1);
			union.addAll(userListTittle2);
			
			if(union.size() ==0 ) {
				return ret;
			}

			Set<String> intersection=  new HashSet<String>(userListTittle1);
			intersection.retainAll(userListTittle2);
			
			ret = ((double)intersection.size())/union.size();

		} catch (Exception e) {
			System.out.println(e);
		}
		return ret;
	}
	

}
