package com.khoa.quach.norcaltraindatabasebuilder;

import android.app.Activity;
import android.os.Bundle;
import android.os.Handler;
import android.os.Message;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.Button;
import android.widget.ProgressBar;
import android.widget.ScrollView;
import android.widget.TextView;

public class DatabaseBuilderMainActivity extends Activity implements Handler.Callback {

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.database_builder_layout);
		
		final ProgressBar build_progress = (ProgressBar)findViewById(R.id.progress_bar);
		build_progress.setVisibility(View.INVISIBLE);
		
		final Button start_build = (Button) findViewById(R.id.button_start_build);
		start_build.setOnClickListener(new View.OnClickListener() {
		    @Override
		    public void onClick(View v) {
		        buildDatabaseNow(v);
		        
		    }
		});
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {

		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.database_builder_main, menu);
		return true;
	}

	@Override
	public boolean onOptionsItemSelected(MenuItem item) {
		// Handle action bar item clicks here. The action bar will
		// automatically handle clicks on the Home/Up button, so long
		// as you specify a parent activity in AndroidManifest.xml.
		int id = item.getItemId();
		if (id == R.id.action_settings) {
			return true;
		}
		return super.onOptionsItemSelected(item);
	}
	
	/**
	 * Do building the database here
	 */
	public void buildDatabaseNow(View v) {
		
		final CalTrainDatabaseHelper m_caltrainDb = new CalTrainDatabaseHelper(v.getContext());
		
		final TextView status_text = (TextView)findViewById(R.id.text_view_status);
		final ProgressBar build_progress = (ProgressBar)findViewById(R.id.progress_bar);
		final Button start_build = (Button) findViewById(R.id.button_start_build);
		final ScrollView scroll_view = (ScrollView) findViewById(R.id.scroll_view);
		scroll_view.fullScroll(View.FOCUS_DOWN);
		build_progress.setVisibility(View.VISIBLE);
		start_build.setEnabled(false);
		
		final Handler handler = new Handler(this);
		
	    Runnable runnable = new Runnable() {
	       
	        public void run() {
	   
                handler.post(new Runnable(){
                    public void run() {
                       status_text.append("Staring building the database...\n");
                    }
                });
            
	        	// Do work here
	    		m_caltrainDb.SetupDatabaseTables(handler);
	            
	            handler.post(new Runnable(){
                    public void run() {
                    	status_text.append("Done\n");
                       	build_progress.setVisibility(View.INVISIBLE);
       		        	start_build.setEnabled(true);
                    }
                });
	            
		        
	        }
	    };
	    new Thread(runnable).start();
		
	}

	@Override
	public boolean handleMessage(Message msg) {
		
		final TextView status_text = (TextView)findViewById(R.id.text_view_status);
		Bundle bundle = msg.getData();
        String text = bundle.getString("status");
		status_text.append(text + "\n");
		
		return false;
		
	}

}
