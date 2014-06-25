package com.khoa.quach.norcaltraindatabasebuilder;

import android.app.Activity;
import android.app.ActionBar;
import android.app.Fragment;
import android.net.wifi.WifiConfiguration.Status;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.ProgressBar;
import android.widget.TextView;
import android.os.Build;

public class DatabaseBuilderMainActivity extends Activity {

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
		    	start_build.setEnabled(false);
		    	build_progress.setVisibility(View.VISIBLE);
		        buildDatabaseNow(v);
		        start_build.setEnabled(true);
		        build_progress.setVisibility(View.INVISIBLE);
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
		
		final TextView status_text = (TextView)findViewById(R.id.text_view_status);
		
		for (int i = 0; i < 100; i++) {
			try {
				status_text.setText("count " + i);
				Thread.sleep(1000);
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

}
