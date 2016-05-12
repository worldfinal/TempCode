package com.frc.test;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.alibaba.fastjson.JSON;
import com.frc.mag.parse.IConstants;
import com.frc.mag.parse.Processor;
import com.frc.mag.send.BaseSender;
import com.frc.mag.thread.QueryThread;
import com.frc.util.IOUtil;

public class JavaSample {
	private static final Logger log = LoggerFactory.getLogger("WF");
	public static String PATH = IConstants.TESTPATH;
	public static long MOD = 100000000;
	public static String COMMON_ATTR = "Ti,Y,CC,Id,AA.AuId,AA.AfId,J.JId,C.CId,F.FId,RId";

	@Test
	public void testMap() {
		Map result = new HashMap();
		for (int i = 0; i < 4; i++) {
			Map rs = new HashMap();
			rs.put(String.format("WF%d1", i), i * 3);
			rs.put(String.format("WF%d2", i), i * 4);
			rs.put("WF", i);
			result.putAll(rs);
		}
		System.out.println(result);
	}
	
	@Test(dataProvider = "d2")
	public void process(String ttl, String expr, String attributes) {
		QueryThread thread = new QueryThread(expr, attributes, IConstants.MAX_COUNT, "0");
		thread.start();
		try {
			thread.join();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		long l = System.currentTimeMillis();
		String fileName = String.format("%s\\data_%s_%d.json", PATH, ttl, l % MOD);
		System.out.println("Writing to file: " + fileName);
		IOUtil.writeStringToFile(fileName, thread.getResult().toString());
	}

	@Test(dataProvider = "test")
	public void withoutThread(String ttl, String expr, String attributes) {
		Map result = BaseSender.queryData(expr, attributes, IConstants.MAX_COUNT, "0");
		long l = System.currentTimeMillis();
		String fileName = String.format("%s\\data_%s_%d.json", PATH, ttl, l % MOD);
		System.out.println("Writing to file: " + fileName);
		IOUtil.writeStringToFile(fileName, result.toString());
	}

	@Test
	public void test() {
		log.debug("WF");
		StringBuffer sb = new StringBuffer();
		String path = String.format("%s\\data_evaluate_10367231.json", PATH);
		String txt = IOUtil.readTxtFile(path);
		Map obj = JSON.parseObject(txt);
		List arr = (List) obj.get("entities");
		if (arr != null && arr.size() > 0) {
			Map data = (Map) arr.get(0);
			List ridArr = (List) data.get("RId");
			if (ridArr != null) {
				log.debug("{}", ridArr.size());
				String cond = "RId=" + ridArr.get(0);
				for (int i = 1; i < ridArr.size(); i++) {
					String str = String.format("Or(%s,RId=%s)", cond, ridArr.get(i));
					cond = str;
				}
				log.debug(cond);
			}
		}
	}

	@DataProvider(name = "d1")
	public Object[][] createData1() {
		return new Object[][] { { "evaluate", "And(RId=2099768249,Y>2010)", COMMON_ATTR } };
	}

	@DataProvider(name = "d2")
	public Object[][] createData2() {
		return new Object[][] { { "evaluate", "Composite(AA.AuId=2161289042)", COMMON_ATTR } };
	}

	@DataProvider(name = "d3")
	public Object[][] createData3() {
		return new Object[][] { { "evaluate", "And(Id=2058571314,Y>2010)", COMMON_ATTR } };
	}

	@DataProvider(name = "d4")
	public Object[][] createData4() {
		return new Object[][] { { "evaluate", "Or(RId=2258634911,RId=2179118004)", "Id" } };
	}
	
	@DataProvider(name = "rid")
	public Object[][] createDataRid() {
		return new Object[][] { { "evaluate", "Or(RId=2258634911,RId=2179118004,RId=2129395888)", COMMON_ATTR } };
	}

	@DataProvider(name = "test")
	public Object[][] createDataTest() {
		return new Object[][] { { "evaluate",
				"Or(Or(Or(Or(Or(Or(Or(Or(Or(Or(Or(Or(Or(Or(Or(Or(Or(Or(Or(Or(RId=2258634911,RId=2179118004),RId=2129395888),RId=1591513229),RId=2016914525),RId=2120450154),RId=2120167959),RId=2163579355),RId=2150280783),RId=2074888021),RId=2176145895),RId=2011705879),RId=1517771483),RId=2008949478),RId=2113371678),RId=2150654203),RId=1588452043),RId=1583422162),RId=2170434667),RId=2038297613),RId=2280079006)",
				COMMON_ATTR } };
	}

	public static void main(String[] args) {
		HttpClient httpclient = HttpClients.createDefault();

		try {
			String url = "https://api.projectoxford.ai/academic/v1.0/evaluate";
			int idx = url.lastIndexOf("/");
			String ttl = url.substring(idx + 1);
			String expr = "And(RId=2099768249,Y>2010)";
			String attributes = "Ti,Y,CC,Id,AA.AuId,AA.AfId,J.JId,C.CId,F.FId,RId";

			URIBuilder builder = new URIBuilder(url);

			builder.setParameter("expr", expr);
			builder.setParameter("model", "latest");
			builder.setParameter("attributes", attributes);
			builder.setParameter("count", "20");
			builder.setParameter("offset", "0");

			URI uri = builder.build();
			HttpGet request = new HttpGet(uri);
			request.setHeader("Ocp-Apim-Subscription-Key", "667a998acb5b4eae8a4e10fcdd1a00ff");

			HttpResponse response = httpclient.execute(request);
			HttpEntity entity = response.getEntity();

			if (entity != null) {
				String rs = EntityUtils.toString(entity);
				System.out.println(rs);
				long l = System.currentTimeMillis();
				String fileName = String.format("%s\\data_%s_%d.json", PATH, ttl, l % MOD);
				System.out.println("Writing to file: " + fileName);
				IOUtil.writeStringToFile(fileName, "/*\n", false);
				IOUtil.writeStringToFile(fileName, expr + "\n", true);
				IOUtil.writeStringToFile(fileName, attributes + "\n", true);
				IOUtil.writeStringToFile(fileName, url + "\n", true);
				IOUtil.writeStringToFile(fileName, "*/\n", true);
				IOUtil.writeStringToFile(fileName, rs, true);
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
