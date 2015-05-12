package com.cabuda.store;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;

public class HBaseAdmin {
	private HTable _table = null;
	/**
	 * 
	 * @param isTemp true temp_info  false permanency_info
	 */
	public HBaseAdmin(boolean isTemp){
		Configuration conf = new Configuration();
		conf.addResource("conf/hbase-site.xml");
		
		try{
			if(isTemp){
				_table = new HTable(conf,"temp_info");
			}else{
				_table = new HTable(conf,"permanency_info");
			}	
		}catch(IOException e){
			
		}
	}
}
