package com.cabuda.util;

import java.util.NoSuchElementException;
import java.util.StringTokenizer;

public class PhotoData {
	private long _timeTemp;
	private String _fileName;
	private long _fileSize;
	private String _fileMD5;

	private byte[] _image;
	private String _fileID;

	// 从服务器初始化
	public PhotoData(String fileName, byte[] image) {
		_image = image;
		_fileName = fileName;
		_timeTemp = System.currentTimeMillis();
		_fileSize = image.length;
		_fileMD5 = MD5Util.getStringMD5(_image);

		StringBuffer sb = new StringBuffer();
		sb.append(String.valueOf(_timeTemp));
		sb.append("_");
		sb.append(String.valueOf(_fileSize));
		sb.append("_");
		sb.append(_fileName);
		sb.append("_");
		sb.append(_fileMD5);
		_fileID = sb.toString();
	}

	// 从数据库初始化
	public PhotoData(byte[] image, String fileID) {
		_image = image;
		_fileID = fileID;

		StringTokenizer st = new StringTokenizer(_fileID, "_");
		try {
			_timeTemp = Long.parseLong(st.nextToken());
			_fileSize = Long.parseLong(st.nextToken());
			_fileName = st.nextToken();
			_fileMD5 = st.nextToken();
		} catch (NoSuchElementException e) {
			Logger.write("PhotoData 29:fileID read error");
		}
	}

	public int getFileSize() {
		return (int) _fileSize;
	}
	public String getFileID() {
		return _fileID;
	}

	public byte[] getImageByteArray() {
		return _image;
	}

	public String getFileName() {
		return _fileName;
	}
}
