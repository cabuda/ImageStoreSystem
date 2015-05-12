package com.cabuda.util;

import java.util.NoSuchElementException;
import java.util.StringTokenizer;

public class PhotoLocationData {
	
	private String _sequenceLocation;
	private long _offset;
	// 服务器初始化
	public PhotoLocationData(String location) {
		try {
			StringTokenizer st = new StringTokenizer(location, "_");
			_sequenceLocation = st.nextToken();
			_offset = Integer.parseInt(st.nextToken());
		} catch (NoSuchElementException e) {
			Logger.write("PhotoLocationData 17:photolocation read error");
		}
	}
	// 数据库初始化
	public PhotoLocationData(String sequenceLocation, long offset){
		_sequenceLocation = sequenceLocation;
		_offset = offset;
	}
	public String getSequenceFileLocation() {
		return _sequenceLocation;
	}
	public long getOffset() {
		return _offset;
	}
	public String getLocation(){
		String location = null;
		StringBuffer sb = new StringBuffer();
		sb.append(_sequenceLocation);
		sb.append("_");
		sb.append(String.valueOf(_offset));
		
		location = sb.toString();
				
		return location;
	}
}
