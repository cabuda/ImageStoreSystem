package com.cabuda.admin;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.cabuda.store.AdminHBase;
import com.cabuda.store.AdminHDFS;
import com.cabuda.util.Logger;
import com.cabuda.util.PhotoData;
import com.cabuda.util.PhotoLocationData;

public class Admin {

	private AdminHBase hbase_temp = null;
	private AdminHBase hbase_per = null;
	private AdminHDFS hdfs = null;

	public Admin() {
		hbase_temp = new AdminHBase(true);
		hbase_per = new AdminHBase(false);
		hdfs = new AdminHDFS();
	}

	public void autoClean() {
		ResultScanner rss = hbase_temp.getAll();
		if (rss == null) {
			Logger.write("Admin 21: rss is null");
			return;
		}
		List<PhotoData> photos = new ArrayList<PhotoData>();
		Map<String, PhotoLocationData> plds = null;

		int size = 0;
		for (Result result : rss) {
			String fileID = new String(result.getRow());
			PhotoData photo = hdfs.readSingleFile(fileID);
			photos.add(photo);
			size += photo.getFileSize();
			if (size >= Logger.BLOCK_SIZE) {
				plds = hdfs.write(photos);
				@SuppressWarnings("rawtypes")
				Iterator iter = plds.entrySet().iterator();
				while (iter.hasNext()) {
					@SuppressWarnings("rawtypes")
					Entry entry = (Entry) iter.next();
					String id = (String) entry.getKey();
					PhotoLocationData pld = (PhotoLocationData) entry
							.getValue();
					hbase_per.writePhotoLocationData(id, pld);

					hbase_temp.delete(id);
					hdfs.delete(id);
				}
				photos.clear();
				plds = null;
				size = 0;
			}
		}
	}

	public String read(String fileID) {
		byte[] image = null;
		String location = hbase_temp.readLocation(fileID);
		if (location != null) {
			PhotoData pd = hdfs.readSingleFile(fileID);
			if (pd != null) {
				image = pd.getImageByteArray();
			} else {
				Logger.write("Admin 76:pd is null");
			}
		} else {
			PhotoLocationData pld = hbase_per.readPhotoLocationData(fileID);
			if(pld != null){
				PhotoData pd = hdfs.read(pld);
				if (pd != null) {
					image = pd.getImageByteArray();
				} else {
					Logger.write("Admin 83:pd is null");
				}
			} else {
				Logger.write("Admin 86:pld is null");
			}
		}

		return Base64.getEncoder().encodeToString(image);
	}

	public String write(String photo, String fileName) {
		byte[] image = Base64.getDecoder().decode(photo);
		PhotoData pd = new PhotoData(fileName, image);

		hdfs.writeSingleFile(pd);
		hbase_temp.writeLocation(pd.getFileID());
		
		return pd.getFileID();
	}
}
