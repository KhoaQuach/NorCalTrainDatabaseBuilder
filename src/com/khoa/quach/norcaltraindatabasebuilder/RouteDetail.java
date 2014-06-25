package com.khoa.quach.norcaltraindatabasebuilder;

import java.text.SimpleDateFormat;
import java.util.Date;

public class RouteDetail {
	
	String depart_station_name;
	String arrival_station_name;
    String route_number;
    String route_depart;
    String route_arrive;
    String route_duration;
    String route_name;
    String route_direction;
    String route_start_date;
    String route_end_date;
    String route_service_id;
    boolean need_transfer = false;
    boolean direct_route = false;
    TransferDetail route_transfer = null;
    
    // Empty constructor
    public RouteDetail(){}
    
    /*
     * Calculate the time difference between depart and arrival times 
     */
    private void calculateAndSetDuration() {
    	
        try {	
        	
            long diff = RouteDetail.TimeDifference(route_depart, route_arrive);

            long diffMinutes = diff / (60 * 1000);
            if ( 60 <= diffMinutes ) {
            	long diffHours = diff / (60 * 60 * 1000) % 24;
            	diffMinutes = diff / (60 * 1000) % 60;
            	if (0 <= diffMinutes && diffMinutes <= 9) {
            		this.route_duration = Long.toString(diffHours) + ":0" + Long.toString(diffMinutes);
            	}
            	else {
            		this.route_duration = Long.toString(diffHours) + ":" + Long.toString(diffMinutes);
            	}
            } 
            else {
            	this.route_duration = "0:" + Long.toString(diffMinutes); 
            }
        }
        catch (Exception e) 
        {
            // TODO: handle exception
        } 
    }
    
    public String getArrivalStationName() {
    	return arrival_station_name;
    }
    
    public void setArrivalStationName(String station_name) {
    	arrival_station_name = station_name;
    }
    
    public String getDepartStationName() {
    	return depart_station_name;
    }
    
    public void setDepartStationName(String station_name) {
    	depart_station_name = station_name;
    }
    
    public boolean getDirectRoute() {
    	return direct_route;
    }
    
    public void setDirectRoute(boolean _direct_route) {
    	direct_route = _direct_route;
    }
    
    
    public boolean getNeedTransfer() {
    	return need_transfer;
    }
    
    public void setNeedTransfer(boolean _need_transfer) {
    	need_transfer = _need_transfer;
    }
    
    public String getRouteDirection() {
    	return route_direction;
    }
    
    public void setRouteDirection(String _direction) {
    	route_direction = _direction;
    }
    
    public String getRouteNumber(){
        return this.route_number;
    }
     
    public void setRouteNumber(String _route_number){
        this.route_number = _route_number;
    }
     
    public String getRouteFormatedTimeDepart(){
    	
    	String departTime = "";
    	
    	 try {
         	// Convert to 12 hour format
         	SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
             Date depart = sdf.parse(this.route_depart);
             departTime = new SimpleDateFormat("hh:mm a").format(depart);
     	} catch (java.text.ParseException e) {
 			departTime = route_depart;
 		}
    	 
        return departTime;
    }

    public String getRouteDepart(){
    	
    	return route_depart;
    }
     
    public void setRouteDepart(String _route_depart){
        this.route_depart = _route_depart;
        
        if ( (!this.route_depart.isEmpty()) && (this.route_arrive != null) && (!this.route_arrive.isEmpty())) {
        	calculateAndSetDuration();	
        }
    }
     
    public String getRouteFormatedTimeArrive(){
    	
    	String arriveTime = "";
    	
    	try {
        	// Convert to 12 hour format
        	SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
            Date arrive = sdf.parse(this.route_arrive);
            arriveTime = new SimpleDateFormat("hh:mm a").format(arrive);
    	} catch (java.text.ParseException e) {
			arriveTime = route_arrive;
		}
    	
        return arriveTime;
    }

    public String getRouteArrive(){
    	
    	return route_arrive;
    }
     
    public void setRouteArrive(String _route_arrive){
        this.route_arrive = _route_arrive;
        
        if ( (this.route_depart != null) && (!this.route_depart.isEmpty()) && (!this.route_arrive.isEmpty())) {
        	calculateAndSetDuration();
        }
    }
    
    public String getRouteDuration(){
        return this.route_duration;
    }
     
    public void setRouteDuration(String _route_duration){
        this.route_duration = _route_duration;
    }
    
    public String getRouteName(){
        return this.route_name;
    }
     
    public void setRouteName(String _route_name){
        this.route_name = _route_name;
    }
    
    public String getRouteEndDate(){
        return this.route_end_date;
    }
     
    public void setRouteEndDate(String _date){
        route_end_date = _date;
    }
    
    public String getRouteServiceId(){
        return this.route_service_id;
    }
     
    public void setRouteServiceId(String _service_id){
        route_service_id = _service_id;
    }
    
    public String getRouteStartDate(){
        return this.route_start_date;
    }
     
    public void setRouteStartDate(String _date){
        route_start_date = _date;
    }
    
    public TransferDetail getRouteTransfer(){
        return this.route_transfer;
    }
     
    public void setRouteTransfer(TransferDetail _route_transfer){
        this.route_transfer = _route_transfer;
    }
    
    /*
     * Calculate the difference between two input parameters
     */
    public static long TimeDifference(String depart_time, String arrival_time) {
    
    	SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
    	
    	Date d1 = null;
        Date d2 = null;
        long diff = 0;
        
        if ( depart_time.isEmpty() || arrival_time.isEmpty()) {
        	return 0;
        }
        
        try {
        
            d1 = format.parse(arrival_time);
            d2 = format.parse(depart_time);

            // in milliseconds
            diff = d1.getTime() - d2.getTime();
            
        }
        catch (Exception e) 
        {
            // TODO: handle exception
        } 
        
    	return diff;
    }
    
}

