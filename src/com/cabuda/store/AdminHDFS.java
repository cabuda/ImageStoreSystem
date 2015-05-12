package com.cabuda.store;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import com.cabuda.util.Logger;
import com.cabuda.util.PhotoData;
import com.cabuda.util.PhotoLocationData;

public class AdminHDFS {
	private Configuration _conf = null;
	private FileSystem _fileSystem = null;

	public AdminHDFS() {
		_conf = new Configuration();
		_conf.addResource("conf/core-site.xml");
		_conf.addResource("conf/hdfs-site.xml");
		_conf.addResource("conf/mapred-site.xml");

		try {
			_fileSystem = FileSystem.get(_conf);
		} catch (IOException e) {
			Logger.write("HDFSAdmin 23: init HDFSAdmin error");
		}
	}

	public boolean delete(String fileID) {
		boolean isDeleted = false;
		Path path = new Path(Logger.HDFS_URL + "temp/" + fileID);
		try {
			_fileSystem.delete(path, true);
			isDeleted = true;
		} catch (IOException e) {
			Logger.write("HDFSAdmin 34: delete fail");
			isDeleted = false;
		}
		return isDeleted;
	}

	public PhotoData readSingleFile(String fileID) {
		Path path = new Path(Logger.HDFS_URL + "temp/" + fileID);
		PhotoData photo = null;
		try {
			FSDataInputStream in = _fileSystem.open(path);
			@SuppressWarnings("deprecation")
			int length = (int) _fileSystem.getLength(path);
			byte[] image = new byte[length];
			in.read(image);
			in.close();
			photo = new PhotoData(image, fileID);
		} catch (IOException e) {
			Logger.write("HDFSAdmin 54:read error");
		}
		return photo;
	}

	public boolean writeSingleFile(PhotoData photo) {
		boolean isWrited = false;
		byte[] image = photo.getImageByteArray();
		String id = photo.getFileID();
		Path path = new Path(Logger.HDFS_URL + "temp/" + id);
		try {
			FSDataOutputStream out = _fileSystem.create(path);
			out.write(image);
			out.sync();
			out.close();

			isWrited = true;
		} catch (IOException e) {
			isWrited = false;
			Logger.write("HDFSAdmin 76:write error");
		}
		return isWrited;
	}

	public PhotoData read(PhotoLocationData pld) {
		PhotoData photo = null;
		Path path = new Path(pld.getSequenceFileLocation());
		try {
			SequenceFile.Reader reader = new SequenceFile.Reader(_fileSystem,
					path, _conf);
			Text key = new Text();
			BytesWritable value = new BytesWritable();
			if (reader.getPosition() != pld.getOffset()) {
				reader.seek(pld.getOffset());
			}
			reader.next(key, value);
			System.out.println(value.getLength());
			photo = new PhotoData(value.getBytes(), key.toString());
			reader.close();
		} catch (IOException e) {
			Logger.write("HDFSAdmin 100: get reader fail");
		}

		return photo;
	}

	public Map<String, PhotoLocationData> write(List<PhotoData> photos) {
		StringBuffer sb = new StringBuffer();
		sb.append("Block");
		sb.append(String.valueOf(System.currentTimeMillis()));
		sb.append(".seq");
		Path path = new Path(sb.toString());
		
		Text _key = new Text();
		BytesWritable _value = new BytesWritable();
		
		Map<String, PhotoLocationData> plds = new HashMap<String, PhotoLocationData>();
		SequenceFile.Writer writer = null;
		try {
			writer = SequenceFile.createWriter(_fileSystem, _conf, path,
					_key.getClass(), _value.getClass());
		} catch (IOException e1) {
			Logger.write("HDFSAdmin 126:create writer fail");
		}
		for (PhotoData photo : photos) {
			_key.set(photo.getFileID());
			_value.set(new BytesWritable(photo.getImageByteArray()));
			System.out.println(_value.getLength());
			try {
				long offset = writer.getLength();
				writer.append(_key, _value);
			
				PhotoLocationData pld = new PhotoLocationData(path.toString(), offset);
				plds.put(photo.getFileID(), pld);
			} catch (IOException e) {
				Logger.write("HDFSAdmin 139:"
						+ photo.getFileID() + " write fail");
			}
		}
		IOUtils.closeStream(writer);

		return plds;
	}
}
