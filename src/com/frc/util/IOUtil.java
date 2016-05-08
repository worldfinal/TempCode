package com.frc.util;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class IOUtil {
	public static InputStream getResourceAsStream(final String name) {
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		return getResourceAsStream(name, cl);
	}

	private static InputStream getResourceAsStream(final String name, final ClassLoader classloader) {
		return classloader.getResourceAsStream(name);
	}

	public static byte[] readBytesFromClasspath(final String name) {
		InputStream input = null;

		try {
			input = getResourceAsStream(name);
			return toBytes(input);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			IOUtil.closeQuietly(input);
		}
		return null;
	}

	public static byte[] readBytesFromFile(final String name) throws IOException {
		InputStream input = null;
		try {
			input = new FileInputStream(name);
			return toBytes(input);
		} finally {
			IOUtil.closeQuietly(input);
		}
	}

	public static byte[] readBytesFromFile(final String name, final boolean loadFromClassPath) throws IOException {
		byte[] content = null;

		if (loadFromClassPath) {
			content = IOUtil.readBytesFromClasspath(name);
		} else {
			content = IOUtil.readBytesFromFile(name);
		}

		return content;
	}

	public static void closeQuietly(final InputStream input) {
		if (input != null) {
			try {
				input.close();
			} catch (IOException e) {

			}
		}
	}

	private static byte[] toBytes(final InputStream input) throws IOException {
		byte[] buffer = new byte[input.available()];
		input.read(buffer);

		return buffer;
	}

	public static final boolean fileExists(String fileName, boolean loadFromClasspath) {
		boolean result = false;
		if (loadFromClasspath) {
			InputStream is = null;

			try {
				is = getResourceAsStream(fileName);
				is.close();
				result = true;
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {

			}
		} else {
			File file = new File(fileName);
			if (file.exists()) {
				result = true;
			}
		}
		return result;
	}

	public static String readTxtFile(String filePath) {
		StringBuffer fileContent = new StringBuffer();
		try {
			String encoding = "UTF-8";
			File file = new File(filePath);
			if (file.isFile() && file.exists()) { // 判断文件是否存在
				InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);// 考虑到编码格式
				BufferedReader bufferedReader = new BufferedReader(read);
				String lineTxt = null;
				while ((lineTxt = bufferedReader.readLine()) != null) {
					fileContent.append(lineTxt);
				}
				read.close();
			} else {
				System.out.println("找不到指定的文件");
			}
		} catch (Exception e) {
			System.out.println("读取文件内容出错");
			e.printStackTrace();
		}
		return fileContent.toString();
	}

	public static void writeStringToFile(String path, String str) {
		try {
			File file = new File(path);
			if (!file.exists()) {
				file.createNewFile();
			}
			FileOutputStream out = new FileOutputStream(file, false);
			StringBuffer sb = new StringBuffer();
			sb.append(str);
			out.write(sb.toString().getBytes("utf-8"));
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void writeStringToFile(String path, String str, boolean append) {
		try {
			File file = new File(path);
			if (!file.exists()) {
				file.createNewFile();
			}
			FileOutputStream out = new FileOutputStream(file, append);
			StringBuffer sb = new StringBuffer();
			sb.append(str);
			out.write(sb.toString().getBytes("utf-8"));
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static int writeBytesToFile(byte[] data, String name) {
		int n = data.length;
		try {
			DataOutputStream out = new DataOutputStream(new FileOutputStream(name));
			out.write(data);
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return data.length;
	}
}
