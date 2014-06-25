package com.khoa.quach.norcaltraindatabasebuilder;

public class Stop {
	
	//private variables
    String stop_id;
    String stop_code;
    String stop_name;
    String stop_desc;
    Double stop_lat;
    Double stop_lon;
    String stop_zone_id;
    String stop_url;
    String stop_location_type;
    String stop_parent_station;
    String stop_platform_code;
     
    // Empty constructor
    public Stop(){}
    
    // constructor
    public Stop(String _stop_id, 
    		    String _stop_code, 
    		    String _stop_name,
    		    String _stop_desc,
    		    Double _stop_lat,
    		    Double _stop_lon,
    		    String _stop_zone_id,
    		    String _stop_url,
    		    String _stop_location_type,
    		    String _stop_parent_station,
    		    String _stop_platform_code){
    	this.stop_id = _stop_id;
        this.stop_code = _stop_code;
        this.stop_name = _stop_name;
        this.stop_desc = _stop_desc;
        this.stop_lat = _stop_lat;
        this.stop_lon = _stop_lon;
        this.stop_zone_id = _stop_zone_id;
        this.stop_url = _stop_url;
        this.stop_location_type = _stop_location_type;
        this.stop_parent_station = _stop_parent_station;
        this.stop_platform_code = _stop_platform_code;
    }
     
    public String getStopId(){
        return this.stop_id;
    }
     
    public void setStopId(String _stop_id){
        this.stop_id = _stop_id;
    }
     
    public String getStopCode(){
        return this.stop_code;
    }
     
    public void setStopCode(String _stop_code){
        this.stop_code = _stop_code;
    }
     
    public String getStopName(){
        return this.stop_name;
    }
     
    public void setStopName(String _stop_name){
        this.stop_name = _stop_name;
    }
    
    public String getStopDesc(){
        return this.stop_desc;
    }
     
    public void setStopDesc(String _stop_desc){
        this.stop_desc = _stop_desc;
    }
    
    public Double getStopLat(){
        return this.stop_lat;
    }
     
    public void setStopLat(Double _stop_lat){
        this.stop_lat = _stop_lat;
    }
    
    public Double getStopLon(){
        return this.stop_lon;
    }
     
    public void setStopLon(Double _stop_lon){
        this.stop_lon = _stop_lon;
    }
    
    public String getStopUrl(){
        return this.stop_url;
    }
     
    public void setStopUrl(String _stop_url){
        this.stop_url = _stop_url;
    }
    
    public String getStopParentStation(){
        return this.stop_parent_station;
    }
     
    public void setStopParentStation(String _stop_parent_station){
        this.stop_parent_station = _stop_parent_station;
    }
    
    public String getStopLocationType(){
        return this.stop_location_type;
    }
     
    public void setStopLocationType(String _stop_location_type){
        this.stop_location_type = _stop_location_type;
    }
    
    public String getStopPlatformCode(){
        return this.stop_platform_code;
    }
     
    public void setStopPlatformCode(String _stop_platform_code){
        this.stop_platform_code = _stop_platform_code;
    }
    
    public String getStopZoneId(){
        return this.stop_zone_id;
    }
     
    public void setStopZoneId(String _stop_zone_id){
        this.stop_zone_id = _stop_zone_id;
    }
    
}
