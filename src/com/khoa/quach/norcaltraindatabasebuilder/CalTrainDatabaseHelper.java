package com.khoa.quach.norcaltraindatabasebuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.content.res.AssetManager;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteOpenHelper;
import android.os.Bundle;
import android.os.Handler;
import android.os.Message;
import android.widget.TextView;
 
public class CalTrainDatabaseHelper extends SQLiteOpenHelper {

	private static Context myContext;
	private static Handler uiHandler;
	
	List<String> m_stopNameList = new ArrayList<String>();
	List<Stop> m_stopList = new ArrayList<Stop>();
	
	// Initial version
    private static final int DATABASE_VERSION = 1;
 
    // Database Name
    private static final String DATABASE_NAME = "Caltrain_GTFS";
 
    private static File DATABASE_FILE;	
	
	//
	// Following table is the aggregation data from caltrain gtfs tables; it's
	// created with purpose to speed up the query to display different route
	// to UI
	//
	private static final String TABLE_CALTRAIN_SCHEDULES	= "caltrain_schedules";
	
	//
	// caltrain_schedules columns
	//
	private static final String CALTRAIN_SCHEDULE_ROUTE_NUMBER			= "route_number";
	private static final String CALTRAIN_SCHEDULE_ROUTE_DIRECTION		= "route_direction";
	private static final String CALTRAIN_SCHEDULE_DEPART_STOP_NAME		= "depart_stop_name";
	private static final String CALTRAIN_SCHEDULE_ARRIVAL_STOP_NAME		= "arrival_stop_name";
	private static final String CALTRAIN_SCHEDULE_DEPART_TIME			= "depart_time";
	private static final String CALTRAIN_SCHEDULE_ARRIVAL_TIME			= "arrival_time";
	private static final String CALTRAIN_SCHEDULE_ROUTE_TYPE			= "route_type";
	private static final String CALTRAIN_SCHEDULE_SERVICE_ID			= "service_id";
	private static final String CALTRAIN_SCHEDULE_START_DATE			= "start_date";
	private static final String CALTRAIN_SCHEDULE_END_DATE				= "end_date";
	private static final String CALTRAIN_SCHEDULE_TRANSFER_STOP_NAME	= "transfer_stop_name";
	private static final String CALTRAIN_SCHEDULE_TRANSFER_ROUTE_NUMBER	= "transfer_route_number";
	private static final String CALTRAIN_SCHEDULE_TRANSFER_DEPART_TIME	= "transfer_depart_time";
	private static final String CALTRAIN_SCHEDULE_TRANSFER_ARRIVAL_TIME	= "transfer_arrival_time";
	
	//
	// Following tables are mirrors the data from the gtfs files
	//
	
    //
    // All the table names
    //
    private static final String TABLE_AGENCY			= "agency";
    private static final String TABLE_CALENDAR			= "calendar";
    private static final String TABLE_CALENDAR_DATES	= "calendar_dates";
    private static final String TABLE_FARE_ATTRIBUTES	= "fare_attributes";
    private static final String TABLE_FARE_RULES		= "fare_rules";
    private static final String TABLE_ROUTES 			= "routes";
    private static final String TABLE_SHAPES			= "shapes";
    private static final String TABLE_STOPS 			= "stops";
    private static final String TABLE_STOP_TIMES 		= "stop_times";
    private static final String TABLE_TRIPS 			= "trips";
    
    //
    // agency column names
    //
    private static final String AGENCY_NAME			= "name";
    private static final String AGENCY_URL 			= "url";
    private static final String AGENCY_TIMEZONE 	= "timezone";
    private static final String AGENCY_LANGUAGE 	= "language";
    private static final String AGENCY_PHONE 		= "phone";
    private static final String AGENCY_ID 			= "id";
    
    //
    //
    // calendar_dates column names
    //
    private static final String CALENDAR_DATES_SERVICE_ID		= "service_id";
    private static final String CALENDAR_DATES_DATE				= "date";
    private static final String CALENDAR_DATES_EXCEPTION_TYPE	= "exception_type";
    
    //
    // calendar column names
    //
    private static final String CALENDAR_SERVICE_ID		= "service_id";
    private static final String CALENDAR_MONDAY			= "monday";
    private static final String CALENDAR_TUESDAY		= "tuesday";
    private static final String CALENDAR_WEDNESDAY		= "wednesday";
    private static final String CALENDAR_THURSDAY		= "thursday";
    private static final String CALENDAR_FRIDAY			= "friday";
    private static final String CALENDAR_SATURDAY		= "saturday";
    private static final String CALENDAR_SUNDAY			= "sunday";
    private static final String CALENDAR_START_DATE		= "start_date";
    private static final String CALENDAR_END_DATE		= "end_date";
    
    //
    // fare_attributes column names
    //
    private static final String FARE_ATTRIBUTES_FARE_ID				= "fare_id";
    private static final String FARE_ATTRIBUTES_PRICE				= "price";
    private static final String FARE_ATTRIBUTES_CURRENCY_TYPE		= "currency_type";
    private static final String FARE_ATTRIBUTES_PAYMENT_METHOD		= "payment_method";
    private static final String FARE_ATTRIBUTES_TRANSFERS			= "transfers";
    private static final String FARE_ATTRIBUTES_TRANSFER_DURATION	= "transfer_duration";
    
    //
    // fare_rules column names
    //
    private static final String FARE_RULES_FARE_ID			= "fare_id";
    private static final String FARE_RULES_ROUTE_ID			= "route_id";
    private static final String FARE_RULES_ORIGIN_ID		= "origin_id";
    private static final String FARE_RULES_DESTINATION_ID	= "destination_id";
    
    //
    // shapes column names
    //
    private static final String SHAPES_ID				= "shape_id";
    private static final String SHAPES_PT_LAT			= "shape_pt_lat";
    private static final String SHAPES_PT_LON			= "shape_pt_lon";
    private static final String SHAPES_PT_SEQUENCE		= "shape_pt_sequence";
    private static final String SHAPES_DIST_TRAVELED	= "shape_dist_traveled";
    
    // stops Table Columns names
    //
    private static final String STOPS_ID 				= "stop_id";
    private static final String STOPS_CODE 				= "stop_code";
    private static final String STOPS_NAME 				= "stop_name";
    private static final String STOPS_DESC 				= "stop_desc";
    private static final String STOPS_LAT 				= "stop_lat";
    private static final String STOPS_LON 				= "stop_lon";
    private static final String STOPS_ZONE_ID 			= "zone_id";
    private static final String STOPS_URL 				= "stop_url";
    private static final String STOPS_LOC_TYPE 			= "location_type";
    private static final String STOPS_PARENT_STATION 	= "parent_station";
    private static final String STOPS_PLATFORM_CODE 	= "platform_code";
    
    //
    // trips Table Columns names
    //
    // route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,block_id,shape_id
    //
    private static final String TRIPS_ROUTE_ID		= "route_id";
    private static final String TRIPS_SERVICE_ID 	= "service_id";
    private static final String TRIPS_TRIP_ID 		= "trip_id";
    private static final String TRIPS_HEAD_SIGN 	= "trip_headsign";
    private static final String TRIPS_SHORT_NAME 	= "trip_short_name";
    private static final String TRIPS_DIRECTION_ID 	= "direction_id";
    private static final String TRIPS_BLOCK_ID 		= "block_id";
    private static final String TRIPS_SHAPE_ID 		= "shape_id";
    
    //
    // stop_time Table Columns names
    //
    // trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,dropoff_type
    //
    private static final String STOP_TIMES_TRIP_ID 			= "trip_id";
    private static final String STOP_TIMES_ARRIVAL_TIME 	= "arrival_time";
    private static final String STOP_TIMES_DEPARTURE_TIME 	= "departure_time";
    private static final String STOP_TIMES_STOP_ID 			= "stop_id";
    private static final String STOP_TIMES_STOP_SEQUENCE 	= "stop_sequence";
    private static final String STOP_TIMES_PICKUP_TYPE 		= "pickup_type";
    private static final String STOP_TIMES_DROPOFF_TYPE 	= "dropoff_type";
    
    //
    // routes Table Columns names
    //
    // route_id,route_short_name,route_long_name,route_desc,route_type,route_url,route_color
    //
    private static final String ROUTES_ID 			= "route_id";
    private static final String ROUTES_SHORT_NAME 	= "route_short_name";
    private static final String ROUTES_LONG_NAME 	= "route_long_name";
    private static final String ROUTES_DESC 		= "route_desc";
    private static final String ROUTES_TYPE 		= "route_type";
    private static final String ROUTES_URL 			= "route_url";
    private static final String ROUTES_COLOR 		= "route_color";
    
    public CalTrainDatabaseHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
        myContext = context;
        
        SQLiteDatabase db = null;
		try {
			db = getReadableDatabase();
			if (db != null) {
		  		db.close();
			}
		
			DATABASE_FILE = context.getDatabasePath(DATABASE_NAME);
		} catch (SQLiteException e) {
		} finally {
			if (db != null && db.isOpen()) {
				db.close();
			}
		}
    }
 
    // Creating Tables
    @Override
    public void onCreate(SQLiteDatabase db) {
	    // Create tables
    	createTables(db);
    }
 
    // Upgrading database
    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
    	dropAllTables(db);
    	onCreate(db);
        
    }
    
    /*
     * Aggregate individual trip to caltrain_schedules table
     */
    private void AggregateCaltrainSchedule(String depart_station, String arrival_station, String direction, ScheduledEnum selectedSchedule, boolean ignore_transfer) throws Exception {
    	
    	String route_number = "", route_name = "";
    	
    	UpdateStatus("AggregateCaltrainSchedule() is starting...");
    	UpdateStatus("depart="+depart_station+"; arrival="+arrival_station+"; direction="+direction+"; scheduled enum="+selectedSchedule.toString());
    	
    	// Data is temporarily held here
		Map<String, RouteDetail> routeTempDetail = new LinkedHashMap<String, RouteDetail>();
		RouteDetail newRouteDetail;
		
    	String selectQuery = BuildGetCaltrainScheduleQueryStatement(depart_station, arrival_station, direction, selectedSchedule);
    	
		try {
			SQLiteDatabase db = this.getReadableDatabase();
			Cursor cursor = db.rawQuery(selectQuery, null);
			
			if ( cursor.moveToFirst() ) {
				
			    do { 
			    	
			    	//
			    	// Marshal the data
			    	//
			    	
			    	if (depart_station.compareTo(cursor.getString(0).trim()) == 0) {
			    		
			    		newRouteDetail = new RouteDetail();
			    		
			    		newRouteDetail.setRouteDirection(direction);
			    		
			    		//
			    		// The row data belong to source station, save them into a hashable list
			    		//
			    		newRouteDetail.setDepartStationName(cursor.getString(0).trim());
			    		route_number = cursor.getString(1).trim();
			    		newRouteDetail.setRouteNumber(route_number);
			    		newRouteDetail.setRouteDepart(cursor.getString(2).trim());
			    		newRouteDetail.setRouteName(cursor.getString(3).trim());
			    		newRouteDetail.setRouteStartDate(cursor.getString(4).trim());
			    		newRouteDetail.setRouteEndDate(cursor.getString(5).trim());
			    		newRouteDetail.setRouteServiceId(cursor.getString(6).trim());
			    		newRouteDetail.setRouteArrive("Transfer(s)");
			    		newRouteDetail.setDirectRoute(false);
			    		newRouteDetail.setNeedTransfer(false);
			    		
			    		// Save it in a temporary hashtable object
			    		routeTempDetail.put(route_number, newRouteDetail);
			    		
			    	}
			    	else if (arrival_station.compareTo(cursor.getString(0).trim()) == 0) {
			    		
			    		//
			    		// The row data belongs to destination station, get the existing route detail
			    		// based on the route number from the hashable list
			    		//
			    		route_number = cursor.getString(1).trim();
			    		route_name = cursor.getString(3).trim();
			    		newRouteDetail = routeTempDetail.get(route_number);
			    		
			    		if ( (newRouteDetail != null) && (route_name.equals(newRouteDetail.getRouteName()))) {
			    			newRouteDetail.setArrivalStationName(cursor.getString(0).trim());
			    			newRouteDetail.setRouteArrive(cursor.getString(2).trim());
			    			newRouteDetail.setDirectRoute(true);
			    			newRouteDetail.setNeedTransfer(false);
			    		}
			    		else {
			    			if (newRouteDetail == null && ignore_transfer == false) {
			    				
			    				newRouteDetail = new RouteDetail();
					    		
			    				newRouteDetail.setRouteDirection(direction);
			    				
					    		//
					    		// The row data belong to destination station, save them into a hashable list
					    		//
					    		route_number = cursor.getString(1).trim();
					    		newRouteDetail.setArrivalStationName(cursor.getString(0).trim());
					    		newRouteDetail.setRouteNumber(route_number);
					    		newRouteDetail.setRouteArrive(cursor.getString(2).trim());
					    		newRouteDetail.setRouteName(cursor.getString(3).trim());
					    		newRouteDetail.setRouteStartDate(cursor.getString(4).trim());
					    		newRouteDetail.setRouteEndDate(cursor.getString(5).trim());
					    		newRouteDetail.setRouteServiceId(cursor.getString(6).trim());
					    		newRouteDetail.setRouteDepart("Transfer(s)");
					    		newRouteDetail.setDirectRoute(false);
					    		newRouteDetail.setNeedTransfer(true);
			    				
			    				// Save it in a temporary hashtable object
					    		routeTempDetail.put(route_number, newRouteDetail);
			    			}
			    		}
			    		
			    	}
			    	
			    } while ( cursor.moveToNext() );
			    
			    int position = 0;
			    int least_position = 0;
			    
			    // Now add items into the list in order
			    for (Map.Entry<String, RouteDetail> entry : routeTempDetail.entrySet()) {
			    	
			        RouteDetail routeDetail = entry.getValue();
			        if (routeDetail.getNeedTransfer()) {
			        
			        	UpdateStatus("Need transfer...");
			        	
			        	List<String> keyList = new ArrayList<String>(routeTempDetail.keySet());
			        	
			        	// Determine if it has any transfer routes within 30 minutes,
			        	// if it does, build the transfer route list and add to detail list,
			        	// otherwise, just ignore it
			        	
			        	RouteDetail test_entry = null;
			        	for( int i = position; i >= least_position; i--) {
			        		
			        		String route_id = keyList.get(i);
			        	    test_entry = routeTempDetail.get(route_id);
			        	   
			        	    if (!test_entry.getNeedTransfer()) {
			        	    	
			        	    	TransferDetail transfer = getTransferDetail(test_entry.getRouteDepart(), test_entry.getRouteNumber(), routeDetail.getRouteArrive(), routeDetail.getRouteNumber(), direction);
			        	    	if ((transfer != null) && (60*1000*30 <= routeDetail.TimeDifference(transfer.getArrivalTime(), transfer.getDepartTime()))) {
			        	    		break;
			        	    	}
			        	    	
			        	    	// Only do transfer if less than 30 minutes
			        	    	if (transfer != null) {
			        	    		
			        	    		routeDetail.setDepartStationName(test_entry.getDepartStationName());
			        	    		routeDetail.setRouteNumber(test_entry.getRouteNumber());
						    		routeDetail.setRouteDepart(test_entry.getRouteDepart());
						    		
			        	    		routeDetail.setRouteTransfer(transfer);
			        	    		
			        	    		InsertScheduleIntoTable(routeDetail);
			        	    		
			        	    		least_position = i;
			        	    		
			        	    		break;
			        	    		
			        	    	}
			        	    	
			        	    }
			       
			        	}
			        }
			        else {
			        	if (routeDetail.getDirectRoute()) {
			        		InsertScheduleIntoTable(routeDetail);
			        	}
			        }
			    
			        position++;
			    }
			}
			
		} catch(Exception e) {
			throw e;
		}
    }
    
    /*
     * Aggregate data to Caltrain schedule table 
     */
    private void AggregateCaltrainSchedules(ScheduledEnum scheduledDate) {
    	
    	String direction = "";
    	
    	try {
			List<String> station_names = getAllStopNames();
			
			for (int i = 0; i < station_names.size(); i++) {
				for (int j = i + 1; j < station_names.size(); j++) {
					
					if (j < i) direction = "NB";
					else direction = "SB";
					
					boolean ignore_transfer = Math.abs(j-i)<=1?true:false;
					
					if ( i != j ) {
						AggregateCaltrainSchedule(station_names.get(i), station_names.get(j), direction, scheduledDate, ignore_transfer);
					}
				}
			}
			
			for (int i = station_names.size()-1; 0 <= i; i--) {
				for (int j = i; 0 <= j; j--) {
					
					if (j < i) direction = "NB";
					else direction = "SB";
					
					boolean ignore_transfer = Math.abs(i-j)<=1?true:false;
					
					if ( i != j ) {
						AggregateCaltrainSchedule(station_names.get(i), station_names.get(j), direction, scheduledDate, ignore_transfer);
					}
				}
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
     
    /*
     * Build appropriate query statement to get data from database per trip
     */
    private String BuildGetCaltrainScheduleQueryStatement(String depart_station, String arrival_station, String direction, ScheduledEnum selectedSchedule) {
    	
    	String queryStatement = "", contents = "";
    	
    	switch (selectedSchedule) {
    	case WEEKDAY:
    		
    		contents = getFileContents("queries/get_weekday_routes.txt");
	   		queryStatement = String.format(contents, direction, depart_station, arrival_station);
    		break;
    		
    	case SATURDAY:
    		
    		contents = getFileContents("queries/get_saturday_routes.txt");
	   		queryStatement = String.format(contents, direction, depart_station, arrival_station);
    		break;
    		
    	case SUNDAY:
    		
    		contents = getFileContents("queries/get_sunday_routes.txt");
	   		queryStatement = String.format(contents, direction, depart_station, arrival_station);
    		break;
    		
    	}
    	
    	return queryStatement;
    }
    
    /*
     * Build sql query to return any transfer between two stations, order by the nearest depart station to farthest
     */
    private String BuildGetTransferDetailQueryStatement(String depart_time, String depart_trip_number, String arrival_time, String destination_trip_number, String direction) {
    	
    	String queryStatement = "", contents = "";
    	String order = direction.equals("NB")?"ASC":"DESC";
    	
    	contents = getFileContents("queries/transfer.txt");
    	
	   	queryStatement = String.format(contents, depart_time, arrival_time, direction, destination_trip_number, depart_time, arrival_time, direction, depart_trip_number, order);
    	
    	return queryStatement;
    }
    
    /*
     * Create all tables
     */
    private void createTables(SQLiteDatabase db) {
    	
    	createAgencyTable(db);
    	createCalendarDatesTable(db);
    	createCalendarTable(db);
    	createFareAttributesTable(db);
    	createFareRulesTable(db);
    	createShapesTable(db);
    	createRoutesTable(db);
    	createStopsTable(db);
    	createStopTimesTable(db);
    	createTripsTable(db);
    	createCaltrainSchedulesTable(db);
    	
    }
    
    /*
     * Create caltrain_schedules table
     */
    private void createCaltrainSchedulesTable(SQLiteDatabase db) {
    	  
    	if ( !isTableExists(TABLE_CALTRAIN_SCHEDULES) ) {
    		
			// Create table
			String CREATE_AGENCY_TABLE = "CREATE TABLE " + TABLE_CALTRAIN_SCHEDULES + "("
	                + CALTRAIN_SCHEDULE_ROUTE_NUMBER + " VARCHAR(32) NOT NULL," 
	                + CALTRAIN_SCHEDULE_ROUTE_DIRECTION + " VARCHAR(32) NOT NULL," 
	        		+ CALTRAIN_SCHEDULE_DEPART_STOP_NAME + " VARCHAR(255) NOT NULL,"
	        		+ CALTRAIN_SCHEDULE_ARRIVAL_STOP_NAME + " VARCHAR(255) NOT NULL,"
	        		+ CALTRAIN_SCHEDULE_DEPART_TIME + " VARCHAR(32) NOT NULL,"
	                + CALTRAIN_SCHEDULE_ARRIVAL_TIME + " VARCHAR(32) NOT NULL,"
	        		+ CALTRAIN_SCHEDULE_ROUTE_TYPE + " VARCHAR(32) NOT NULL,"
	        		+ CALTRAIN_SCHEDULE_SERVICE_ID + " VARCHAR(255) NOT NULL,"
	        		+ CALTRAIN_SCHEDULE_START_DATE + " VARCHAR(32) NOT NULL,"
	                + CALTRAIN_SCHEDULE_END_DATE + " VARCHAR(32) NOT NULL,"
	                + CALTRAIN_SCHEDULE_TRANSFER_STOP_NAME + " VARCHAR(255) NULL,"
	        		+ CALTRAIN_SCHEDULE_TRANSFER_ROUTE_NUMBER + " VARCHAR(32) NULL,"
	        		+ CALTRAIN_SCHEDULE_TRANSFER_DEPART_TIME + " VARCHAR(32) NULL,"
	                + CALTRAIN_SCHEDULE_TRANSFER_ARRIVAL_TIME + " VARCHAR(32) NULL"
	        		+ ");"
	        		+ "CREATE INDEX idx_depart_stop_name ON " + TABLE_CALTRAIN_SCHEDULES + "(" + CALTRAIN_SCHEDULE_DEPART_STOP_NAME + ");"
	        		+ "CREATE INDEX idx_arrival_stop_name ON " + TABLE_CALTRAIN_SCHEDULES + "(" + CALTRAIN_SCHEDULE_ARRIVAL_STOP_NAME + ");"
					+ "CREATE INDEX idx_start_date ON " + TABLE_CALTRAIN_SCHEDULES + "(" + CALTRAIN_SCHEDULE_START_DATE + ");"
					+ "CREATE INDEX idx_end_date ON " + TABLE_CALTRAIN_SCHEDULES + "(" + CALTRAIN_SCHEDULE_END_DATE + ");";
			db.execSQL(CREATE_AGENCY_TABLE);	
			
    	}
    	
    }
    
    /*
     * Create agency table
     */
    private void createAgencyTable(SQLiteDatabase db) {
    	  
    	if ( !isTableExists(TABLE_AGENCY) ) {
    		
			// Create table
			String CREATE_AGENCY_TABLE = "CREATE TABLE " + TABLE_AGENCY + "("
	                + AGENCY_NAME + " VARCHAR(255) NOT NULL," 
	        		+ AGENCY_URL + " VARCHAR(255),"
	                + AGENCY_TIMEZONE + " VARCHAR(10),"
	                + AGENCY_LANGUAGE + " VARCHAR(32),"
	                + AGENCY_PHONE + " VARCHAR(64),"
	                + AGENCY_ID + " VARCHAR(32)"
	        		+ ");";
					
			db.execSQL(CREATE_AGENCY_TABLE);	
			
    	}
    	
    }
    
    /*
     * Create calendar_dates table
     */
    private void createCalendarDatesTable(SQLiteDatabase db) {
    	
    	if ( !isTableExists(this.TABLE_CALENDAR_DATES) ) {
    		
			// Create table
			String CREATE_CALENDAR_DATES_TABLE = "CREATE TABLE " + TABLE_CALENDAR_DATES + "("
	                + CALENDAR_DATES_SERVICE_ID + " VARCHAR(255) NOT NULL," 
	        		+ CALENDAR_DATES_DATE + " TEXT,"
	                + CALENDAR_DATES_EXCEPTION_TYPE + " VARCHAR(10)"
	        		+ ");"
					+ "CREATE INDEX service_id_idx ON " + TABLE_CALENDAR_DATES + "(" + CALENDAR_DATES_SERVICE_ID + ");"
					+ "CREATE INDEX exception_type_idx ON " + TABLE_CALENDAR_DATES + "(" + CALENDAR_DATES_EXCEPTION_TYPE + ");";
			
			db.execSQL(CREATE_CALENDAR_DATES_TABLE);	
			
    	}
    	
    }
    
    /*
     * Create calendar table
     */
    private void createCalendarTable(SQLiteDatabase db) {
        
    	if ( !isTableExists(this.TABLE_CALENDAR) ) {	
    	
			// Create table
			String CREATE_CALENDAR_TABLE = "CREATE TABLE " + TABLE_CALENDAR + "("
	                + CALENDAR_SERVICE_ID + " VARCHAR(255) NOT NULL PRIMARY KEY," 
	        		+ CALENDAR_MONDAY + " TINYINT,"
	                + CALENDAR_TUESDAY + " TINYINT,"
	                + CALENDAR_WEDNESDAY + " TINYINT,"
	                + CALENDAR_THURSDAY + " TINYINT,"
	                + CALENDAR_FRIDAY + " TINYINT,"
	                + CALENDAR_SATURDAY + " TINYINT,"
	                + CALENDAR_SUNDAY + " TINYINT,"
	                + CALENDAR_START_DATE + " TEXT,"
	                + CALENDAR_END_DATE + " TEXT"
	        		+ ")";
	        
			db.execSQL(CREATE_CALENDAR_TABLE);
			
    	}
    	
    
    }
    
    /*
     * Create fare_attributes table
     */
    private void createFareAttributesTable(SQLiteDatabase db) {
	
    	if ( !isTableExists(this.TABLE_FARE_ATTRIBUTES) ) {
    		
			// Create table
			String CREATE_FARE_ATTRIBUTES_TABLE = "CREATE TABLE " + TABLE_FARE_ATTRIBUTES + "("
	                + FARE_ATTRIBUTES_FARE_ID + " VARCHAR(255) NOT NULL," 
	        		+ FARE_ATTRIBUTES_PRICE + " VARCHAR(32),"
	                + FARE_ATTRIBUTES_CURRENCY_TYPE + " VARCHAR(10),"
	                + FARE_ATTRIBUTES_PAYMENT_METHOD + " TINYINT,"
	                + FARE_ATTRIBUTES_TRANSFERS + " TINYINT,"
	                + FARE_ATTRIBUTES_TRANSFER_DURATION + " INT NULL"
	        		+ ")";
	        
			db.execSQL(CREATE_FARE_ATTRIBUTES_TABLE);
			
    	}
    	
    }
    
    /*
     * Create fare_rules table
     */
    private void createFareRulesTable(SQLiteDatabase db) {
    		
    	if ( !isTableExists(this.TABLE_FARE_RULES) ) {
    		
			// Create table
			String CREATE_FARE_RULES_TABLE = "CREATE TABLE " + TABLE_FARE_RULES + "("
	                + FARE_RULES_FARE_ID + " VARCHAR(32) NOT NULL," 
	        		+ FARE_RULES_ROUTE_ID + " VARCHAR(10),"
	                + FARE_RULES_ORIGIN_ID + " VARCHAR(10),"
	                + FARE_RULES_DESTINATION_ID + " VARCHAR(10)"
	        		+ ")";
	        
			db.execSQL(CREATE_FARE_RULES_TABLE);
    	}
    	
    		
    }
    
    /*
     * Create shapes table
     */
    private void createShapesTable(SQLiteDatabase db) {
    	
    	if ( !isTableExists(this.TABLE_SHAPES) ) {
    		
			// Create table
			String CREATE_SHAPES_TABLE = "CREATE TABLE " + TABLE_SHAPES + "("
	                + SHAPES_ID + " VARCHAR(255) NOT NULL," 
	        		+ SHAPES_PT_LAT + " NUMERIC,"
	                + SHAPES_PT_LON + " NUMERIC,"
	                + SHAPES_PT_SEQUENCE + " INT,"
	                + SHAPES_DIST_TRAVELED + " NUMERIC NULL"
	        		+ ")";
	        
			db.execSQL(CREATE_SHAPES_TABLE);
    	}
    	
    }
    
    /*
     * Create routes table
     */
    private void createRoutesTable(SQLiteDatabase db) {
    
    	if ( !isTableExists(TABLE_ROUTES) ) {
    		
			// Create table
			String CREATE_ROUTES_TABLE = "CREATE TABLE " + TABLE_ROUTES + "("
	                + ROUTES_ID + " VARCHAR(255) NOT NULL PRIMARY KEY," 
	        		+ ROUTES_SHORT_NAME + " VARCHAR(255),"
	                + ROUTES_LONG_NAME + " VARCHAR(255),"
	                + ROUTES_DESC + " VARCHAR(255),"
	                + ROUTES_TYPE + " smallint,"
	                + ROUTES_URL + " VARCHAR(255) NULL,"
	                + ROUTES_COLOR + " VARCHAR(255) NULL"
	        		+ ")";
	        
			db.execSQL(CREATE_ROUTES_TABLE);
    	}
    	
    }
    
    /*
     * Create stop_time table
     */
    private void createStopTimesTable(SQLiteDatabase db) {
    
    	if ( !isTableExists(this.TABLE_STOP_TIMES) ) {
    		
			// Create table
			String CREATE_STOP_TIME_TABLE = "CREATE TABLE " + TABLE_STOP_TIMES + "("
	                + STOP_TIMES_TRIP_ID + " VARCHAR(255) NOT NULL," 
	        		+ STOP_TIMES_ARRIVAL_TIME + " TEXT,"
	                + STOP_TIMES_DEPARTURE_TIME + " TEXT,"
	                + STOP_TIMES_STOP_ID + " VARCHAR(255) NOT NULL,"
	                + STOP_TIMES_STOP_SEQUENCE + " SMALLINT NOT NULL,"
	                + STOP_TIMES_PICKUP_TYPE + " SMALLINT,"
	                + STOP_TIMES_DROPOFF_TYPE + " SMALLINT"
	        		+ ");"
	        		+ "CREATE INDEX trip_id_idx ON " + TABLE_STOP_TIMES + "(" + STOP_TIMES_TRIP_ID + ");"
					+ "CREATE INDEX stop_id_idx ON " + TABLE_STOP_TIMES + "(" + STOP_TIMES_STOP_ID + ");"
					+ "CREATE INDEX pickup_type ON " + TABLE_STOP_TIMES + "(" + STOP_TIMES_PICKUP_TYPE + ");"
					+ "CREATE INDEX dropoff_type_idx ON " + TABLE_STOP_TIMES + "(" + STOP_TIMES_DROPOFF_TYPE + ")";
	        
			db.execSQL(CREATE_STOP_TIME_TABLE);
			
    	}
    	
    }
    
    /*
     * Create stops table
     */
    private void createStopsTable(SQLiteDatabase db) {
    
    	if ( !isTableExists(this.TABLE_STOPS) ) {
    		
			// Create stops table
			String CREATE_STOPS_TABLE = "CREATE TABLE " + TABLE_STOPS + "("
	                + STOPS_ID + " VARCHAR(255) NOT NULL PRIMARY KEY," 
	        		+ STOPS_CODE + " VARCHAR(255),"
	                + STOPS_NAME + " VARCHAR(255) NOT NULL,"
	                + STOPS_DESC + " VARCHAR(255),"
	                + STOPS_LAT + " NUMERIC NOT NULL,"
	                + STOPS_LON + " NUMERIC NOT NULL,"
	                + STOPS_ZONE_ID + " VARCHAR(255),"
	                + STOPS_URL + " VARCHAR(255),"
	                + STOPS_LOC_TYPE + " INT,"
	                + STOPS_PARENT_STATION + " VARCHAR(255),"
	                + STOPS_PLATFORM_CODE + " VARCHAR(255)"
	        		+ ");"
					+ "CREATE INDEX zone_id_idx ON " + TABLE_STOPS + "(" + STOPS_ZONE_ID + ");"
					+ "CREATE INDEX lat_idx ON " + TABLE_STOPS + "(" + STOPS_LAT + ");"
					+ "CREATE INDEX lon_idx ON " + TABLE_STOPS + "(" + STOPS_LON + ")";
				
			db.execSQL(CREATE_STOPS_TABLE);
			
    	}
    	
    }
    
    /*
     * Create trips table
     */
    private void createTripsTable(SQLiteDatabase db) {
         
    	if ( !isTableExists(TABLE_TRIPS) ) {
    		
			// Create table
			String CREATE_STRIPS_TABLE = "CREATE TABLE " + TABLE_TRIPS + "("
	                + TRIPS_ROUTE_ID + " VARCHAR(255) NOT NULL," 
	        		+ TRIPS_SERVICE_ID + " VARCHAR(255) NOT NULL,"
	                + TRIPS_TRIP_ID + " VARCHAR(255) NOT NULL PRIMARY KEY,"
	                + TRIPS_HEAD_SIGN + " VARCHAR(255),"
	                + TRIPS_SHORT_NAME + " VARCHAR(255),"
	                + TRIPS_DIRECTION_ID + " TINYINT,"
	                + TRIPS_BLOCK_ID + " VARCHAR(255),"
	                + TRIPS_SHAPE_ID + " VARCHAR(255)"
	        		+ ");"
	        		+ "CREATE INDEX route_id_idx ON " + TABLE_TRIPS + "(" + TRIPS_ROUTE_ID + ");"
					+ "CREATE INDEX service_id_idx ON " + TABLE_TRIPS + "(" + TRIPS_SERVICE_ID + ");"
					+ "CREATE INDEX direction_id_idx ON " + TABLE_TRIPS + "(" + TRIPS_DIRECTION_ID + ");"
					+ "CREATE INDEX shape_idx ON " + TABLE_TRIPS + "(" + TRIPS_SHAPE_ID + ")";
	        
			db.execSQL(CREATE_STRIPS_TABLE);
			
    	}
   
    }
	
	/*
	 * Drop all tables
	 */
	private void dropAllTables(SQLiteDatabase db) {
		
		db.execSQL("DROP TABLE IF EXISTS " + TABLE_AGENCY);
		db.execSQL("DROP TABLE IF EXISTS " + TABLE_CALENDAR_DATES);
		db.execSQL("DROP TABLE IF EXISTS " + TABLE_CALENDAR);
		db.execSQL("DROP TABLE IF EXISTS " + TABLE_FARE_ATTRIBUTES);
		db.execSQL("DROP TABLE IF EXISTS " + TABLE_FARE_RULES);
		db.execSQL("DROP TABLE IF EXISTS " + TABLE_SHAPES);
		db.execSQL("DROP TABLE IF EXISTS " + TABLE_STOPS);
		db.execSQL("DROP TABLE IF EXISTS " + TABLE_STOP_TIMES);
		db.execSQL("DROP TABLE IF EXISTS " + TABLE_ROUTES);
		db.execSQL("DROP TABLE IF EXISTS " + TABLE_TRIPS);
		db.execSQL("DROP TABLE IF EXISTS " + TABLE_CALTRAIN_SCHEDULES);
		
	}
    
	/*
     * Get all station names
     */
    public List<String> getAllStopNames() throws Exception {
        
    	if ( !isTableExists(TABLE_STOPS) ) {
    		
    		SQLiteDatabase db = getWritableDatabase();
    		
    		createStopsTable(db);
    		
    		populateDataToStopsTable();
    	
    	}
    	
    	if ( m_stopNameList.isEmpty() ) {
    		
    		m_stopNameList.clear();
    		
	        String selectQuery = "SELECT " + STOPS_NAME + " FROM " 
	        					+ TABLE_STOPS 
	        					+ " WHERE " + STOPS_CODE + " <> '' "
	        					+ "	AND " + STOPS_ZONE_ID + " <> '' "
	        					+ " AND " + STOPS_PLATFORM_CODE + " = 'NB' "
	        					+ " ORDER BY " + STOPS_LAT + " DESC";
	 
	        try {
		        SQLiteDatabase db = this.getReadableDatabase();
		        Cursor cursor = db.rawQuery(selectQuery, null);
		
		        String stop_name = "";
		        
		        if ( cursor.moveToFirst() ) {
		            do { 
		            	stop_name = cursor.getString(0);
		            	
		            	if ( !stop_name.isEmpty() ) {
		            		m_stopNameList.add(stop_name.trim());
		            	}
		            } while ( cursor.moveToNext() );
		        }
		 
		        return m_stopNameList;
	        } catch(Exception e) {
	        	throw e;
	        }
	        
    	} else {
    		return m_stopNameList;
    	}
    }
    
    /*
     * Get contents of input file name 
     */
    private String getFileContents(String fileName) {
    	AssetManager am = myContext.getAssets();
    	try {
    		
    		InputStream is = am.open(fileName);
    		BufferedReader r = new BufferedReader(new InputStreamReader(is));
    		StringBuilder contents = new StringBuilder();
    		String line;
    		while ((line = r.readLine()) != null) {
    			contents.append(line);
    			contents.append("\n");
    		}
    	
    		return contents.toString();
    	} catch(IOException ioe) {
    		return "";
    	}
    }
    
    /*
     * Get the number of rows in input table 
     */
    public int getTableCount(String tableName) throws Exception {
    	
    	String countQuery = "SELECT  * FROM " + tableName;
    	try {
        
	        SQLiteDatabase db = this.getReadableDatabase();
	        Cursor cursor = db.rawQuery(countQuery, null);
	        
	        int count = cursor.getCount();
	        cursor.close();
	 
	        // return count
	        return count;
	        
    	} catch(Exception e) {
    		throw e;
    	}
    }
 
    /*
     * Obtain the transfer station detail
     */
    public TransferDetail getTransferDetail(String depart_time, String depart_route_id, String arrival_time, String destination_route_id, String direction) throws Exception {
    
    	TransferDetail new_transfer = null;
    	
    	String selectQuery = BuildGetTransferDetailQueryStatement(depart_time, depart_route_id, arrival_time, destination_route_id, direction);
    	
		try {
			SQLiteDatabase db = this.getReadableDatabase();
			Cursor cursor = db.rawQuery(selectQuery, null);
			
			if ( cursor.moveToFirst() ) {
				
				new_transfer = new TransferDetail(TrimWhiteSpacesOrDoubleQuotes(cursor.getString(0)), 
						cursor.getString(1).trim(), 
						cursor.getString(2).trim(),
						cursor.getString(3).trim(),
						cursor.getString(4).trim());
			    
			}
			
		} catch(Exception e) {
			throw e;
		}

    	return new_transfer;
    }
    
    /*
     * Insert route detail into caltrain schedule table
     */
    private void InsertScheduleIntoTable(RouteDetail routeDetail) throws Exception {

    	if ( !isTableExists(TABLE_CALTRAIN_SCHEDULES)) throw new Exception("Caltrain schedule table does not exist");
        
    	if ( routeDetail == null || routeDetail.getRouteDirection().isEmpty() ) return;

    	UpdateStatus("InsertScheduleInfoTable() staring...");
    	UpdateStatus("route number="+routeDetail.getRouteNumber()+"; route direction="+routeDetail.getRouteDirection());
    	UpdateStatus("depart="+routeDetail.getDepartStationName()+"; arrival="+routeDetail.getArrivalStationName());
    	UpdateStatus("depart time="+routeDetail.getRouteDepart()+"; arrival time="+routeDetail.getRouteArrive());
    	UpdateStatus("route name="+routeDetail.getRouteName()+"; service id="+routeDetail.getRouteServiceId());
    	UpdateStatus("start date="+routeDetail.getRouteStartDate()+"; end date="+routeDetail.getRouteEndDate());
    	
    	String line = "";
    	
    	try {
    		
		    ContentValues values = new ContentValues();  
			
		    values.put(CALTRAIN_SCHEDULE_ROUTE_NUMBER, routeDetail.getRouteNumber());  
		    values.put(CALTRAIN_SCHEDULE_ROUTE_DIRECTION, routeDetail.getRouteDirection());  
		    values.put(CALTRAIN_SCHEDULE_DEPART_STOP_NAME, routeDetail.getDepartStationName());  
		    values.put(CALTRAIN_SCHEDULE_ARRIVAL_STOP_NAME, routeDetail.getArrivalStationName());  
		    values.put(CALTRAIN_SCHEDULE_DEPART_TIME, routeDetail.getRouteDepart());  
		    values.put(CALTRAIN_SCHEDULE_ARRIVAL_TIME, routeDetail.getRouteArrive());  
		    values.put(CALTRAIN_SCHEDULE_ROUTE_TYPE, routeDetail.getRouteName());  
		    values.put(CALTRAIN_SCHEDULE_SERVICE_ID, routeDetail.getRouteServiceId());  
		    values.put(CALTRAIN_SCHEDULE_START_DATE, routeDetail.getRouteStartDate());  
		    values.put(CALTRAIN_SCHEDULE_END_DATE, routeDetail.getRouteEndDate());  
		    
		    if (routeDetail.getRouteTransfer() != null) {
		    	UpdateStatus("transfer stop name="+routeDetail.getRouteTransfer().getStopName()+"; transfer arrival route number="+routeDetail.getRouteTransfer().getArrivalRouteNumber());
		    	UpdateStatus("transfer start time="+routeDetail.getRouteTransfer().getDepartTime()+"; transfer arrival time="+routeDetail.getRouteTransfer().getArrivalTime());
		    	values.put(CALTRAIN_SCHEDULE_TRANSFER_STOP_NAME, routeDetail.getRouteTransfer().getStopName());  
			    values.put(CALTRAIN_SCHEDULE_TRANSFER_ROUTE_NUMBER, routeDetail.getRouteTransfer().getArrivalRouteNumber());  
			    values.put(CALTRAIN_SCHEDULE_TRANSFER_DEPART_TIME, routeDetail.getRouteTransfer().getDepartTime());  
			    values.put(CALTRAIN_SCHEDULE_TRANSFER_ARRIVAL_TIME, routeDetail.getRouteTransfer().getArrivalTime());  
		    }
		    
		    this.getWritableDatabase().insert(TABLE_CALTRAIN_SCHEDULES, null, values); 
			
    	} catch	(Exception e) {
			throw e;
		}		
    }
    
    /*
     * Check if table exists
     */
    private boolean isTableExists(String tableName) {
    	
    	try {
	    	SQLiteDatabase db = this.getReadableDatabase();
	
	        Cursor cursor = db.rawQuery("select DISTINCT tbl_name from sqlite_master where tbl_name = '" + tableName + "'", null);
	        if( cursor!=null ) {
	            if( cursor.getCount() > 0 ) {
	            	cursor.close();
	                return true;
	            }
	                        
	            cursor.close();
        }
    	} catch(Exception e) {
    		return false;
    	}
        return false;
    }
    
    /*
     * Aggregate data to build static routes to speed up sql query
     */
    private void populateDataToCaltrainSchedulesTables() throws Exception {
    	
    	UpdateStatus("populateDataToCaltrainSchedulesTable() is starting...");
    	
    	if ( 0 < getTableCount(TABLE_CALTRAIN_SCHEDULES) ) return;
    	
    	//
    	// Aggregate data for all routes
    	//
    	AggregateCaltrainSchedules(ScheduledEnum.WEEKDAY);
    	AggregateCaltrainSchedules(ScheduledEnum.SATURDAY);
    	AggregateCaltrainSchedules(ScheduledEnum.SUNDAY);
    }
    
    /*
     * Parse and insert data to all tables
     */
    private void populateAllDataToTables() throws Exception {
    	
    	UpdateStatus("PopulateAllDataToTables() is starting...");
    	
    	try {
			this.populateDataToAgencyTable();
	    	this.populateDataToCalendarDatesTable();
	    	this.populateDataToCalendarTable();
	    	this.populateDataToFareAttributesTable();
	    	this.populateDataToFareRulesTable();
	    	this.populateDataToRoutesTable();
	    	this.populateDataToShapesTable();
	    	this.populateDataToStopsTable();
	    	this.populateDataToStopTimesTable();
	    	this.populateDataToTripsTable();
	    	
	    	// Need to be done last
	    	this.populateDataToCaltrainSchedulesTables();
    	} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
		}
    }
    
    /*
     * Populate data to agency table from csv file 
     */
    public void populateDataToAgencyTable() throws Exception {
    	
    	UpdateStatus("populateDataToAgencyTable() is starting...");
    	
    	if ( 0 < getTableCount(TABLE_AGENCY) ) return;
        
    	String line = "";
    	
    	try {
    		InputStream is = myContext.getAssets().open(myContext.getResources().getString(R.string.agency_csv));
    		BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
    		
    		// Skip first line
    		br.readLine();
    		
			while ( (line = br.readLine()) != null ) {
				
			    String[] RowData = line.split(",");  
			    ContentValues values = new ContentValues();  
			    values.put(AGENCY_NAME, TrimWhiteSpacesOrDoubleQuotes(RowData[0]));  
			    values.put(AGENCY_URL, TrimWhiteSpacesOrDoubleQuotes(RowData[1]));  
			    values.put(AGENCY_TIMEZONE, TrimWhiteSpacesOrDoubleQuotes(RowData[2]));  
			    values.put(AGENCY_LANGUAGE, TrimWhiteSpacesOrDoubleQuotes(RowData[3]));  
			    values.put(AGENCY_PHONE, TrimWhiteSpacesOrDoubleQuotes(RowData[4]));  
			    values.put(AGENCY_ID, TrimWhiteSpacesOrDoubleQuotes(RowData[5]));  
			    
			    this.getWritableDatabase().insert(TABLE_AGENCY, null, values); 
			    
			}
			
			br.close();  
    	} catch	(Exception e) {
			throw e;
		}	
    	
    }
    
    /*
     * Populate data to calendar_dates table from csv file 
     */
    public void populateDataToCalendarDatesTable() throws Exception {
    	
    	UpdateStatus("populateDataToCalendarDatesTable() is starting...");
    	
    	if ( 0 < getTableCount(TABLE_CALENDAR_DATES) ) return;
        
    	String line = "";
    	
    	try {
    		InputStream is = myContext.getAssets().open(myContext.getResources().getString(R.string.calendar_dates_csv));
    		BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
    		
    		// Skip first line
    		br.readLine();
    		
			while ( (line = br.readLine()) != null ) {
				
			    String[] RowData = line.split(",");  
			    ContentValues values = new ContentValues();  
			    values.put(CALENDAR_DATES_SERVICE_ID, TrimWhiteSpacesOrDoubleQuotes(RowData[0]));  
			    values.put(CALENDAR_DATES_DATE, TrimWhiteSpacesOrDoubleQuotes(RowData[1]));  
			    values.put(CALENDAR_DATES_EXCEPTION_TYPE, TrimWhiteSpacesOrDoubleQuotes(RowData[2]));  
			    
			    this.getWritableDatabase().insert(TABLE_CALENDAR_DATES, null, values); 
			    
			}
			
			br.close();  
    	} catch	(Exception e) {
			throw e;
		}	
    	
    }
    
    /*
     * Populate data to calendar table from csv file 
     */
    public void populateDataToCalendarTable() throws Exception {
    	
    	UpdateStatus("populateDataToCalendarTable() is starting...");
    	
    	if ( 0 < getTableCount(TABLE_CALENDAR) ) return;
        
    	String line = "";
    	
    	try {
    		InputStream is = myContext.getAssets().open(myContext.getResources().getString(R.string.calendar_csv));
    		BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
    		
    		// Skip first line
    		br.readLine();
    		
			while ( (line = br.readLine()) != null ) {
				
			    String[] RowData = line.split(","); 
			    ContentValues values = new ContentValues();  
			    values.put(CALENDAR_SERVICE_ID, TrimWhiteSpacesOrDoubleQuotes(RowData[0]));  
			    values.put(CALENDAR_MONDAY, TrimWhiteSpacesOrDoubleQuotes(RowData[1]));  
			    values.put(CALENDAR_TUESDAY, TrimWhiteSpacesOrDoubleQuotes(RowData[2]));  
			    values.put(CALENDAR_WEDNESDAY, TrimWhiteSpacesOrDoubleQuotes(RowData[3]));  
			    values.put(CALENDAR_THURSDAY, TrimWhiteSpacesOrDoubleQuotes(RowData[4]));  
			    values.put(CALENDAR_FRIDAY, TrimWhiteSpacesOrDoubleQuotes(RowData[5]));  
			    values.put(CALENDAR_SATURDAY, TrimWhiteSpacesOrDoubleQuotes(RowData[6]));  
			    values.put(CALENDAR_SUNDAY, TrimWhiteSpacesOrDoubleQuotes(RowData[7]));  
			    values.put(CALENDAR_START_DATE, TrimWhiteSpacesOrDoubleQuotes(RowData[8]));  
			    values.put(CALENDAR_END_DATE, TrimWhiteSpacesOrDoubleQuotes(RowData[9]));
			    
			    this.getWritableDatabase().insert(TABLE_CALENDAR, null, values); 
			    
			}
			
			br.close();  
			
		} catch(Exception e) {
			throw e;
		}	
    	
    }
    
    /*
     * Populate data to fare_attributes table from csv file 
     */
    public void populateDataToFareAttributesTable() throws Exception {
    	
    	UpdateStatus("populateDataToFareAttributesTable() is starting...");
    	
    	if ( 0 < getTableCount(TABLE_FARE_ATTRIBUTES) ) return;
        
    	String line = "";
    	
    	try {
    		InputStream is = myContext.getAssets().open(myContext.getResources().getString(R.string.fare_attributes_csv));
    		BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
    		
    		// Skip first line
    		br.readLine();
    		
			while ( (line = br.readLine()) != null ) {
				
			    String[] RowData = line.split(","); 
			    ContentValues values = new ContentValues();  
			    values.put(FARE_ATTRIBUTES_FARE_ID, TrimWhiteSpacesOrDoubleQuotes(RowData[0]));  
			    values.put(FARE_ATTRIBUTES_PRICE, TrimWhiteSpacesOrDoubleQuotes(RowData[1]));  
			    values.put(FARE_ATTRIBUTES_CURRENCY_TYPE, TrimWhiteSpacesOrDoubleQuotes(RowData[2]));  
			    values.put(FARE_ATTRIBUTES_PAYMENT_METHOD, TrimWhiteSpacesOrDoubleQuotes(RowData[3]));  
			    values.put(FARE_ATTRIBUTES_TRANSFERS, TrimWhiteSpacesOrDoubleQuotes(RowData[4]));  
			    if (6 <= RowData.length) values.put(FARE_ATTRIBUTES_TRANSFER_DURATION, TrimWhiteSpacesOrDoubleQuotes(RowData[5]));
			    else values.putNull(FARE_ATTRIBUTES_TRANSFER_DURATION);  
			    
			    this.getWritableDatabase().insert(TABLE_FARE_ATTRIBUTES, null, values); 
			    
			}
			
			br.close();  
			
		} catch(Exception e) {
			throw e;
		}	
    	
    }
    
    /*
     * Populate data to fare_rules table from csv file 
     */
    public void populateDataToFareRulesTable() throws Exception {
    	
    	UpdateStatus("populateDataToFareRulesTable() is starting...");
    	
    	if ( 0 < getTableCount(TABLE_FARE_RULES) ) return;
        
    	String line = "";
    	
    	try {
    		InputStream is = myContext.getAssets().open(myContext.getResources().getString(R.string.fare_rules_csv));
    		BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
    		
    		// Skip first line
    		br.readLine();
    		
			while ( (line = br.readLine()) != null ) {
				    
			    String[] RowData = line.split(","); 
			    ContentValues values = new ContentValues();  
			    values.put(FARE_RULES_FARE_ID, TrimWhiteSpacesOrDoubleQuotes(RowData[0]));  
			    values.put(FARE_RULES_ROUTE_ID, TrimWhiteSpacesOrDoubleQuotes(RowData[1]));  
			    values.put(FARE_RULES_ORIGIN_ID, TrimWhiteSpacesOrDoubleQuotes(RowData[2]));  
			    values.put(FARE_RULES_DESTINATION_ID, TrimWhiteSpacesOrDoubleQuotes(RowData[3]));  
			    
			    this.getWritableDatabase().insert(TABLE_FARE_RULES, null, values); 
			    
			}
			
			br.close();  
			
		} catch(Exception e) {
			throw e;
		}	
    	
    }
    
    /*
     * Populate data to calendar table from csv file 
     */
    public void populateDataToShapesTable() throws Exception {
    	
    	UpdateStatus("populateDataToShapesTable() is starting...");
    	
    	if ( 0 < getTableCount(TABLE_SHAPES) ) return;
        
    	String line = "";
    	
    	try {
    		InputStream is = myContext.getAssets().open(myContext.getResources().getString(R.string.shapes_csv));
    		BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
    		
    		// Skip first line
    		br.readLine();
    		
			while ( (line = br.readLine()) != null ) {
				
			    String[] RowData = line.split(","); 
			    ContentValues values = new ContentValues();  
			    values.put(SHAPES_ID, TrimWhiteSpacesOrDoubleQuotes(RowData[0]));  
			    values.put(SHAPES_PT_LAT, TrimWhiteSpacesOrDoubleQuotes(RowData[1]));  
			    values.put(SHAPES_PT_LON, TrimWhiteSpacesOrDoubleQuotes(RowData[2]));  
			    values.put(SHAPES_PT_SEQUENCE, TrimWhiteSpacesOrDoubleQuotes(RowData[3]));  
			    if (5 <= RowData.length) values.put(SHAPES_DIST_TRAVELED, TrimWhiteSpacesOrDoubleQuotes(RowData[4]));  
			    else values.putNull(SHAPES_DIST_TRAVELED);  
			    
			    this.getWritableDatabase().insert(TABLE_SHAPES, null, values); 
			    
			}
			
			br.close();  
			
		}  catch(Exception e) {
			throw e;
		}	
    	
    }
    
    /*
     * Populate data to routes table from csv file 
     */
    public void populateDataToRoutesTable() throws Exception {
    	
    	UpdateStatus("populateDataToRoutesTable() is starting...");
    	
    	if ( 0 < getTableCount(TABLE_ROUTES) ) return;
        
    	String line = "";
    	
    	try {
    		InputStream is = myContext.getAssets().open(myContext.getResources().getString(R.string.routes_csv));
    		BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
    		
    		// Skip first line
    		br.readLine();
    		
			while ( (line = br.readLine()) != null ) {
				
			    String[] RowData = line.split(",");  
			    ContentValues values = new ContentValues();  
			    values.put(ROUTES_ID, TrimWhiteSpacesOrDoubleQuotes(RowData[0]));  
			    values.put(ROUTES_SHORT_NAME, TrimWhiteSpacesOrDoubleQuotes(RowData[1]));  
			    values.put(ROUTES_LONG_NAME, TrimWhiteSpacesOrDoubleQuotes(RowData[2]));  
			    values.put(ROUTES_DESC, TrimWhiteSpacesOrDoubleQuotes(RowData[3]));  
			    values.put(ROUTES_TYPE, TrimWhiteSpacesOrDoubleQuotes(RowData[4])); 
			    if (6 <= RowData.length) values.put(ROUTES_URL, TrimWhiteSpacesOrDoubleQuotes(RowData[5]));  
			    else values.putNull(ROUTES_URL); 
			    if (7 <= RowData.length) values.put(ROUTES_COLOR, TrimWhiteSpacesOrDoubleQuotes(RowData[6]));  
			    else values.putNull(ROUTES_COLOR);  
			    
			    this.getWritableDatabase().insert(TABLE_ROUTES, null, values); 
			    
			}
			
			br.close();  
			
		} catch(Exception e) {
			throw e;
		}
    	
    	
    }
    
    /*
     * Populate data to stops table from csv file 
     */
    public void populateDataToStopsTable() throws Exception {
    	
    	UpdateStatus("populateDataToStopsTable() is starting...");
    	
    	if ( 0 < getTableCount(TABLE_STOPS) ) return;
    	
    	String line = "";
    	
    	try {
    		InputStream is = myContext.getAssets().open(myContext.getResources().getString(R.string.stops_csv));
    		BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
    		
    		// Skip first line
    		br.readLine();
    		
			while ( (line = br.readLine()) != null ) {
				
			    String[] RowData = line.split(",");  
			    
			 // If the platform code is empty, skip the whole line
			    if (11 <= RowData.length) {
				    ContentValues values = new ContentValues();  
				    values.put(STOPS_ID, TrimWhiteSpacesOrDoubleQuotes(RowData[0]));  
				    values.put(STOPS_CODE, TrimWhiteSpacesOrDoubleQuotes(RowData[1]));  
				    values.put(STOPS_NAME, TrimWhiteSpacesOrDoubleQuotes(RowData[2]));  
				    values.put(STOPS_DESC, TrimWhiteSpacesOrDoubleQuotes(RowData[3]));  
				    values.put(STOPS_LAT, Double.parseDouble(TrimWhiteSpacesOrDoubleQuotes(RowData[4])));  
				    values.put(STOPS_LON, Double.parseDouble(TrimWhiteSpacesOrDoubleQuotes(RowData[5])));  
				    values.put(STOPS_ZONE_ID, TrimWhiteSpacesOrDoubleQuotes(RowData[6]));  
				    values.put(STOPS_URL, TrimWhiteSpacesOrDoubleQuotes(RowData[7]));  
				    values.put(STOPS_LOC_TYPE, TrimWhiteSpacesOrDoubleQuotes(RowData[8]));  
				    values.put(STOPS_PARENT_STATION, TrimWhiteSpacesOrDoubleQuotes(RowData[9]));  
			    	values.put(STOPS_PLATFORM_CODE, TrimWhiteSpacesOrDoubleQuotes(RowData[10]));  
			    
			    	this.getWritableDatabase().insert(TABLE_STOPS, null, values); 
			    }
			}
			
			br.close();  
			
		} catch(Exception e) {
			throw e;
		}
    	
    	
    }
    
    /*
     * Populate data to stop times table from csv file 
     */
    public void populateDataToStopTimesTable() throws Exception {
    	
    	UpdateStatus("populateDataToStopTimesTable() is starting...");
    	
    	if ( 0 < getTableCount(TABLE_STOP_TIMES) ) return;
        
    	String line = "";
    	
    	try {
    		InputStream is = myContext.getAssets().open(myContext.getResources().getString(R.string.stop_times_csv));
    		BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
    		
    		// Skip first line
    		br.readLine();
    		
			while ( (line = br.readLine()) != null ) {
				
			    String[] RowData = line.split(",");
			    ContentValues values = new ContentValues();  
			    values.put(STOP_TIMES_TRIP_ID, TrimWhiteSpacesOrDoubleQuotes(RowData[0]));  
			    values.put(STOP_TIMES_ARRIVAL_TIME, TrimWhiteSpacesOrDoubleQuotes(RowData[1]));  
			    values.put(STOP_TIMES_DEPARTURE_TIME, TrimWhiteSpacesOrDoubleQuotes(RowData[2]));  
			    values.put(STOP_TIMES_STOP_ID, TrimWhiteSpacesOrDoubleQuotes(RowData[3]));  
			    values.put(STOP_TIMES_STOP_SEQUENCE, TrimWhiteSpacesOrDoubleQuotes(RowData[4]));  
			    values.put(STOP_TIMES_PICKUP_TYPE, TrimWhiteSpacesOrDoubleQuotes(RowData[5]));  
			    values.put(STOP_TIMES_DROPOFF_TYPE, TrimWhiteSpacesOrDoubleQuotes(RowData[6]));  
			    
			    this.getWritableDatabase().insert(TABLE_STOP_TIMES, null, values); 
			    
			}
			
			br.close();  
			
		} catch(Exception e) {
			throw e;
		}
    
    }
    
    /*
     * Populate data to trips table from csv file 
     */
    public void populateDataToTripsTable() throws Exception {
    	
    	UpdateStatus("populateDataToTripsTable() is starting...");
    	
    	if ( 0 < getTableCount(TABLE_TRIPS) ) return;
    	    
    	String line = "";
    	
    	try {
    		InputStream is = myContext.getAssets().open(myContext.getResources().getString(R.string.trips_csv));
    		BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
    		
    		// Skip first line
    		br.readLine();
    		
			while ( (line = br.readLine()) != null ) {
				
			    String[] RowData = line.split(",");  	    
			    ContentValues values = new ContentValues();  
			    values.put(TRIPS_ROUTE_ID, TrimWhiteSpacesOrDoubleQuotes(RowData[0]));  
			    values.put(TRIPS_SERVICE_ID, TrimWhiteSpacesOrDoubleQuotes(RowData[1]));  
			    values.put(TRIPS_TRIP_ID, TrimWhiteSpacesOrDoubleQuotes(RowData[2]));  
			    values.put(TRIPS_HEAD_SIGN, TrimWhiteSpacesOrDoubleQuotes(RowData[3]));  
			    values.put(TRIPS_SHORT_NAME, TrimWhiteSpacesOrDoubleQuotes(RowData[4]));  
			    values.put(TRIPS_DIRECTION_ID, TrimWhiteSpacesOrDoubleQuotes(RowData[5]));  
			    values.put(TRIPS_BLOCK_ID, TrimWhiteSpacesOrDoubleQuotes(RowData[6]));
			    values.put(TRIPS_SHAPE_ID, TrimWhiteSpacesOrDoubleQuotes(RowData[7]));
			    
			    this.getWritableDatabase().insert(TABLE_TRIPS, null, values); 
			    
			}
			
			br.close();  
			
		} catch(Exception e) {
			throw e;
		}
    	
    	
    }
    
    /*
     * Check if we need to populate data to tables on first run or on database upgrade
     */
    public void SetupDatabaseTables(Handler _uiHandler) {
    	
    	uiHandler = _uiHandler;
    	
    	UpdateStatus("SetupDatabaseTables() is starting...");
    	
    	try
    	{
	    	if ((getTableCount(TABLE_TRIPS)) <= 0 
	    			|| (getTableCount(TABLE_STOPS) <= 0) 
	    			|| (getTableCount(TABLE_STOP_TIMES) <= 0)
	    			|| (getTableCount(TABLE_SHAPES) <= 0)
	    			|| (getTableCount(TABLE_ROUTES) <= 0)
	    			|| (getTableCount(TABLE_FARE_RULES) <= 0)
	    			|| (getTableCount(TABLE_FARE_ATTRIBUTES) <= 0)
	    			|| (getTableCount(TABLE_CALENDAR_DATES) <= 0)
	    			|| (getTableCount(TABLE_CALENDAR) <= 0)
	    			|| (getTableCount(TABLE_CALTRAIN_SCHEDULES) <= 0)
	    			) {
	    		
	    		// Configure data for the first time 
	    		populateAllDataToTables();	
	    	}
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    	
    }
    
    // 
    // Remove any white space or double quotes at beginning and trailing of the input string
    //
    private String TrimWhiteSpacesOrDoubleQuotes(String str) {
    	return str.trim().replaceAll("^\"|\"$", "").trim();
    }
    
    private void UpdateStatus(String msg) {
            	 
    	 Message message = uiHandler.obtainMessage();

         Bundle bundle = new Bundle();
         bundle.putString("status", msg);

         message.setData(bundle);

         uiHandler.sendMessage(message);
         
    }
    
}


