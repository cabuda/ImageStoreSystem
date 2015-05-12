package com.cabuda.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class Logger {
	private static final String fileName = "/home/hadoop/logger.txt";
	public static final String HDFS_URL = "hdfs://node1:49000/user/hadoop/";
	public static final int BLOCK_SIZE = 64 * 1024 * 1024;

	public static void write(String log) {
		try {
			FileWriter writer = new FileWriter(fileName, true);
			writer.write(log + "\n");
			writer.close();
		} catch (IOException e) {
			return;
		}
	}

	public static String read() {
		String str = null;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			StringBuffer sb = new StringBuffer();
			String content = "";
			while (content != null) {
				content = reader.readLine();
				if (content == null) {
					break;
				}
				sb.append(content.trim());
			}
			str = sb.toString();
			reader.close();
			try {
				reader.read();
			} catch (IOException e) {
				return "logger read error";
			}
		} catch (FileNotFoundException e) {
			return "logger read error";
		} catch (IOException e) {
			return "logger read error";
		}

		return str;
	}

}
