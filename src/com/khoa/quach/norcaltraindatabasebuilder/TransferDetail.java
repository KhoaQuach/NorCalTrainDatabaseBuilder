package com.khoa.quach.norcaltraindatabasebuilder;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TransferDetail {

	String stop_name;
    String arrival_route_number;
    String arrival_time;
    String depart_route_number;
    String depart_time;
    
    // Empty constructor
    public TransferDetail(){}
    
    public TransferDetail(String _stop_name, 
    		    String _arrival_route_number, 
    		    String _arrival_time,
    		    String _depart_route_number,
    		    String _depart_time) {
    	this.stop_name = _stop_name;
        this.arrival_route_number = _arrival_route_number;
        this.arrival_time = _arrival_time;
        this.depart_route_number = _depart_route_number;
        this.depart_time = _depart_time;
    }
    
    public String getArrivalRouteNumber(){
        return this.arrival_route_number;
    }
     
    public void setArrivalRouteNumber(String _route_number){
        this.arrival_route_number = _route_number;
    }
    
    public String getDepartRouteNumber(){
        return this.depart_route_number;
    }
     
    public void setDepartRouteNumber(String _route_number){
        this.depart_route_number = _route_number;
    }
    
    public String getArrivalTime(){
        return arrival_time;
    }
     
    public String getFormatedArrivalTime(){
    	
    	String arrivalTime = "";
    	
    	try {
        	// Convert to 12 hour format
        	SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
            Date arrive = sdf.parse(this.arrival_time);
            arrivalTime = new SimpleDateFormat("hh:mm a").format(arrive);
    	} catch (java.text.ParseException e) {
			arrivalTime = arrival_time;
		}
    	
        return arrivalTime;
    }

    public void setArrivalTime(String _arrival_time){
        this.arrival_time = _arrival_time;
    }
    
    public String getDepartTime(){
        return depart_time;
    }
     
    public String getFormatedDepartTime(){
    	
    	String departTime = "";
    	
    	try {
        	// Convert to 12 hour format
        	SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
            Date depart = sdf.parse(this.depart_time);
            departTime = new SimpleDateFormat("hh:mm a").format(depart);
    	} catch (java.text.ParseException e) {
			departTime = depart_time;
		}
    	
        return departTime;
    }

    public void setDepartTime(String _depart_time){
        this.depart_time = _depart_time;
    }
    
    public String getStopName(){
        return this.stop_name;
    }
     
    public void setStopName(String _stop_name){
        this.stop_name = _stop_name;
    }

}




