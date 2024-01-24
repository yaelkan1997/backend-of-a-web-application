/**
 * 
 */
package org.bgu.ise.ddb.items;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.MediaItems;
import org.bgu.ise.ddb.ParentController;
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
@RequestMapping(value = "/items")
public class ItemsController extends ParentController {
	/**
	 * The function copy all the items(title and production year) from the Oracle table MediaItems to the System storage.
	 * The Oracle table and data should be used from the previous assignment
	 */
	@RequestMapping(value = "fill_media_items", method={RequestMethod.GET})
	public void fillMediaItems(HttpServletResponse response){
		Connection connection = null;
		ResultSet rs =null;
		PreparedStatement ps =null;
		List<MediaItems> mediaItems = new ArrayList<MediaItems>();
		
		//Connect to oracle DB to copy all the items to mongoDB
		try 
		{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			connection = DriverManager.getConnection("jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521/oracle", "yaelkan", "abcd");
			connection.setAutoCommit(false);
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		//add  all the mediaItems to Result set
		try {
			String query = "SELECT title,prod_year FROM MediaItems";
			ps = connection.prepareStatement(query);
			rs = ps.executeQuery();
			
			//add all the mediaItems to array
			while (rs.next()) {
				mediaItems.add(new MediaItems(rs.getString(1), rs.getInt(2)));
			}
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				if (ps != null) {
					ps.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		//add each element to mongoDb
		for (int i=0; i<mediaItems.size();i++) {
			if(!isTittleExist(mediaItems.get(i).getTitle()))
			{
				//connect to mongo and add the item
				try {
					MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
					DBCollection  dbCollection = (DBCollection) mongoClient.getDB("FProjectDB").getCollection("MediaItems");

					BasicDBObject item = new BasicDBObject();
					item.put("Title", mediaItems.get(i).getTitle());
					item.put("Prod_Year", mediaItems.get(i).getProdYear());

					dbCollection.insert(item);
					mongoClient.close();
				} catch (Exception e) {
					// TODO: handle exception
					System.out.println(e);
				}
			}
		}
		
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}
	
	private boolean isTittleExist(String title) {
		boolean result = false;
		try {
			MongoClient mongoClient = null;
			mongoClient = new MongoClient( "localhost" , 27017 );
			DBCollection  dbCollection = (DBCollection) mongoClient.getDB("FProjectDB").getCollection("MediaItems");
			BasicDBObject queryResult = new BasicDBObject();
			queryResult.put("Title", title);
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
	 * The function copy all the items from the remote file,
	 * the remote file have the same structure as the films file from the previous assignment.
	 * You can assume that the address protocol is http
	 * @throws IOException 
	 */
	@RequestMapping(value = "fill_media_items_from_url", method={RequestMethod.GET})
	public void fillMediaItemsFromUrl(@RequestParam("url")    String urladdress,
			HttpServletResponse response) throws IOException{
		System.out.println(urladdress);
		//:TODO your implementation
		URL url = new URL(urladdress);
		BufferedReader bufferedReader = null;
		InputStreamReader inputStreamReader=null;
		String line = "";

		try {
			inputStreamReader=new InputStreamReader(url.openStream());
			bufferedReader = new BufferedReader(inputStreamReader);
			while ((line = bufferedReader.readLine()) != null) {
				//split each line to movie title and year production 
				String[] mediaItem = line.split(",");
				try {
					//check if the item exist in the mongoDb 
					if(!isTittleExist(mediaItem[0]))
					{
						try {
							//connect to loacalhost mongoDb
							MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
							DBCollection  dbCollection = (DBCollection) mongoClient.getDB("FProjectDB").getCollection("MediaItems");
							
							//creat new meditem object
							BasicDBObject item = new BasicDBObject();
							item.put("Title",mediaItem[0]);
							item.put("Prod_Year", Integer.parseInt(mediaItem[1]));
							
							//add the new media Item to the collection 
							dbCollection.insert(item);
							mongoClient.close();
						} catch (Exception e) {
							System.out.println(e);
						}
					}
					
				} catch (Exception e) {
					System.out.println(e);
				}
			}
		} catch (Exception e) {
			System.out.println(e);
		}
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
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
		ArrayList<MediaItems> mediaItemsList = new ArrayList<MediaItems>();
		MongoClient mongoClient = null;
		DBCollection  dbCollection= null;
		try {
			// get the media item connection 
			mongoClient = new MongoClient( "localhost" , 27017 );
			dbCollection = mongoClient.getDB("FProjectDB").getCollection("MediaItems");
			
			// get the first N products 
			DBCursor dbCursor = dbCollection.find().limit(topN);

			while (dbCursor.hasNext()) 
			{ 
				DBObject theObj = dbCursor.next();
				mediaItemsList.add(new MediaItems((String) theObj.get("Title") ,(int) theObj.get("Prod_Year")));

			}
			
		} catch (Exception e) {
			System.out.println(e);
		} finally {
			mongoClient.close();
		}

		return mediaItemsList.toArray(new MediaItems[mediaItemsList.size()]);
	}
		

}
