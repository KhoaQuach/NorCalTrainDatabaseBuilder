package com.khoa.quach.norcaltraindatabasebuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import android.content.ContentValues;
import android.content.Context;
import android.content.res.AssetManager;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteOpenHelper;
 
public class CalTrainDatabaseHelper extends SQLiteOpenHelper {

	private static Context myContext;
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
     * Build appropriate query statement to more detail on the selected stations
     */
    private String BuildGetDetailQueryStatement(String source_station_name, String destination_station_name, String direction) {
    	
    	String queryStatement = "", contents = "";
    	
    	contents = getFileContents("queries/getdetail.txt");
	   	queryStatement = String.format(contents, direction, source_station_name, direction, destination_station_name);
    	
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
	                + CALTRAIN_SCHEDULE_TRANSFER_ARRIVAL_TIME + " VARCHAR(32) NULL,"
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
     * Getting single stop station info by station id
     */
    Stop getStopById(String _station_id) throws Exception {
    	
    	try {
    		
	        SQLiteDatabase db = this.getReadableDatabase();
	 
	        Cursor cursor = db.query(TABLE_STOPS, new String[] { 
	        		STOPS_ID,
	                STOPS_CODE,
	                STOPS_NAME,
	                STOPS_DESC,
	                STOPS_LAT,
	                STOPS_LON,
	                STOPS_ZONE_ID,
	                STOPS_URL,
	                STOPS_LOC_TYPE,
	                STOPS_PARENT_STATION,
	                STOPS_PLATFORM_CODE
	                }, STOPS_ID + "=?",
	                new String[] { String.valueOf(_station_id) }, null, null, null, null);
	        
	        if (cursor != null)
	            cursor.moveToFirst();
	 
	        Stop stop = new Stop(cursor.getString(0),
	                			 cursor.getString(1), 
	                			 cursor.getString(2),
	                			 cursor.getString(3),
	                			 Double.parseDouble(cursor.getString(4)),
	                			 Double.parseDouble(cursor.getString(5)),
	                			 cursor.getString(6),
	                			 cursor.getString(7),
	                			 cursor.getString(8),
	                			 cursor.getString(9),
	                			 cursor.getString(10));
	        	return stop;
	        	
    	} catch (Exception e) {
    		throw e;
    	}
    }
    
    /*
     * Get single stop station info by station name
     */
    Stop getStopByName(String _station_name) throws Exception {
    	
    	try {
	        SQLiteDatabase db = this.getReadableDatabase();
	 
	        Cursor cursor = db.query(TABLE_STOPS, new String[] { 
	        		STOPS_ID,
	                STOPS_CODE,
	                STOPS_NAME,
	                STOPS_DESC,
	                STOPS_LAT,
	                STOPS_LON,
	                STOPS_ZONE_ID,
	                STOPS_URL,
	                STOPS_LOC_TYPE,
	                STOPS_PARENT_STATION,
	                STOPS_PLATFORM_CODE
	                }, STOPS_NAME + "=?",
	                new String[] { String.valueOf(_station_name) }, null, null, null, null);
	        
	        if (cursor != null)
	            cursor.moveToFirst();
	 
	        Stop stop = new Stop(cursor.getString(0),
	                			 cursor.getString(1), 
	                			 cursor.getString(2),
	                			 cursor.getString(3),
	                			 Double.parseDouble(cursor.getString(4)),
	                			 Double.parseDouble(cursor.getString(5)),
	                			 cursor.getString(6),
	                			 cursor.getString(7),
	                			 cursor.getString(8),
	                			 cursor.getString(9),
	                			 cursor.getString(10));
	
	        return stop;
	        
    	} catch(Exception e) {
    		throw e;
    	}
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
     * Get all station stops info
     */
    public List<Stop> getAllStops() throws Exception {
        
    	if ( !isTableExists(TABLE_STOPS) ) {
    		
    		SQLiteDatabase db = getWritableDatabase();
    		
    		createStopsTable(db);
    		
    		populateDataToStopsTable();
    	
    	}

    	if (m_stopList.isEmpty()) {
    		
	    	m_stopList.clear();
	    	
	        String selectQuery = "SELECT "
	        		+ STOPS_ID
	        		+ ", "
	        		+ STOPS_CODE
	        		+ ", "
	        		+ STOPS_NAME
	        		+ ", "
	        		+ STOPS_DESC
	        		+ ", "
	        		+ STOPS_LAT
	        		+ ", "
	        		+ STOPS_LON
	        		+ ", "
	        		+ STOPS_ZONE_ID
	        		+ ", "
	        		+ STOPS_URL
	        		+ ", "
	        		+ STOPS_LOC_TYPE
	        		+ ", "
	        		+ STOPS_PARENT_STATION
	        		+ ", "
	        		+ STOPS_PLATFORM_CODE
	        		+ " FROM " + TABLE_STOPS
	        		+ " WHERE " + STOPS_CODE + " <> '' "
        					+ "	AND " + STOPS_ZONE_ID + " <> '' "
        					+ " AND " + STOPS_PLATFORM_CODE + " = 'NB' "
        					+ " ORDER BY " + STOPS_LAT + " DESC";
	 
	        try {
		        SQLiteDatabase db = this.getReadableDatabase();
		        Cursor cursor = db.rawQuery(selectQuery, null);
		        
		        // looping through all rows and adding to list
		        if (cursor.moveToFirst()) {
		            do {
		            	
		                Stop stop = new Stop();
		                stop.setStopId(cursor.getString(0));
		                stop.setStopCode(cursor.getString(1));
		                stop.setStopName(cursor.getString(2));
		                stop.setStopDesc(cursor.getString(3));
		                stop.setStopLat(Double.parseDouble(cursor.getString(4)));
		                stop.setStopLon(Double.parseDouble(cursor.getString(5)));
		                stop.setStopZoneId(cursor.getString(6));
		                stop.setStopUrl(cursor.getString(7));
		                stop.setStopLocationType(cursor.getString(8));
		                stop.setStopParentStation(cursor.getString(9));
		                stop.setStopPlatformCode(cursor.getString(10));
		                
		                // Adding contact to list
		                m_stopList.add(stop);
		                
		            } while (cursor.moveToNext());
		        }
	        
		        return m_stopList;
		        
	        } catch(Exception e) {
	        	throw e;
	        }
	        
    	}
    	
    	return m_stopList;
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
     * Get the station details based on the current source and destination stations, 
     * and wherether it is southbound or northbound
     */
    public ArrayList<String> getStationDetails(String source_station_name, String destination_station_name, String direction) throws Exception {
    
    	ArrayList<String> detailList = new ArrayList<String>();
    	
    	String selectQuery = BuildGetDetailQueryStatement(source_station_name, destination_station_name, direction);
    	
		try {
			SQLiteDatabase db = this.getReadableDatabase();
			Cursor cursor = db.rawQuery(selectQuery, null);
			
			if ( cursor.moveToFirst() ) {
				
			    do { 
			    	detailList.add(cursor.getString(0));
			    	
			    } while ( cursor.moveToNext() );
			}
			
		} catch(Exception e) {
			throw e;
		}

    	return detailList;
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
    	
    	if ( 0 < getTableCount(TABLE_CALTRAIN_SCHEDULES) ) return;
    	
    	//
    	// Aggregate data for all routes
    	//
    	//AggregateCaltrainSchedules(ScheduleEnum.WEEKDAY);
    	//AggregateCaltrainSchedules(ScheduleEnum.SATURDAY);
    	//AggregateCaltrainSchedules(ScheduleEnum.SUNDAY);
    }
    
    /*
     * Parse and insert data to all tables
     */
    private void populateAllDataToTables() throws Exception {
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
    public void SetupDatabaseTables() {
    	
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
}


