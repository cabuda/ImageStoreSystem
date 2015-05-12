package com.cabuda.store;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;

import com.cabuda.util.Logger;
import com.cabuda.util.PhotoLocationData;

public class AdminHBase {

	private HTable _table = null;
	private boolean _isTemp = false;

	/**
	 * 
	 * @param isTemp
	 *            true temp_info false permanency_info
	 */
	public AdminHBase(boolean isTemp) {
		Configuration conf = new Configuration();
		conf.addResource("conf/hbase-site.xml");
		_isTemp = isTemp;
		try {
			if (_isTemp) {
				_table = new HTable(conf, "temp_info");
			} else {
				_table = new HTable(conf, "permanency_info");
			}
		} catch (IOException e) {
			Logger.write("HBaseAdmin 27:get table error");
		}
	}

	public ResultScanner getAll() {
		Scan scan = new Scan();
		ResultScanner rss = null;
		try {
			rss = _table.getScanner(scan);
		} catch (IOException e) {
			Logger.write("HBaseAdmin 46: get scanner fail");
		}
		return rss;
	}

	public boolean delete(String fileID) {
		boolean _isAchieve = false;
		Delete delete = new Delete(fileID.getBytes());
		try {
			_table.delete(delete);
			_isAchieve = true;
		} catch (IOException e) {
			Logger.write("DeleteFromHBase:delete location false");
			_isAchieve = false;
		}
		return _isAchieve;
	}

	public String readLocation(String fileID) {
		String location = null;
		if (!_isTemp) {
			return null;
		}
		Get get = new Get(fileID.getBytes());
		try {
			Result rs = _table.get(get);
			if (!rs.isEmpty()) {
				location = new String(rs.getValue("location".getBytes(), null));
			}
		} catch (IOException e) {
			Logger.write("HBaseAdmin 59:get location false");
		}
		return location;
	}

	public PhotoLocationData readPhotoLocationData(String fileID) {
		if (_isTemp) {
			return null;
		}
		Get get = new Get(fileID.getBytes());
		PhotoLocationData pld = null;
		try {
			Result rs = _table.get(get);
			String location = new String(rs.getValue("location".getBytes(),
					null));
			pld = new PhotoLocationData(location);
		} catch (IOException e) {
			Logger.write("HBaseAdmin 82:get photolocationdata false");
		}
		return pld;
	}

	public boolean writeLocation(String fileID) {
		if (!_isTemp) {
			return false;
		}
		String location = Logger.HDFS_URL + "temp/" + fileID;
		Put put = new Put(fileID.getBytes());
		put.add("location".getBytes(), null, location.getBytes());
		try {
			_table.put(put);
		} catch (RetriesExhaustedWithDetailsException e) {
			Logger.write("HBaseAdmin 95:write location false");
			return false;
		} catch (InterruptedIOException e) {
			Logger.write("HBaseAdmin 98:write location false");
			return false;
		}
		return true;
	}

	public boolean writePhotoLocationData(String fileID, PhotoLocationData pld) {
		if (_isTemp) {
			return false;
		}
		Put put = new Put(fileID.getBytes());
		put.add("location".getBytes(), null, pld.getLocation().getBytes());
		try {
			_table.put(put);
		} catch (RetriesExhaustedWithDetailsException e) {
			Logger.write("HBaseAdmin 113:write photolocationdata false");
			return false;
		} catch (InterruptedIOException e) {
			Logger.write("HBaseAdmin 116:write photolocationdata false");
			return false;
		}
		return true;
	}
}
