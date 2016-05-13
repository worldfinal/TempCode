package com.frc.mag.send;

import java.net.URI;
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

import com.alibaba.fastjson.JSON;
import com.frc.mag.parse.IConstants;
import com.frc.util.IOUtil;

public class BaseSender {
	public static final Logger log = LoggerFactory.getLogger("WF");
	public static final Logger FLOW = LoggerFactory.getLogger("FLOW");
	public static int COUNTER = 1;
	public static String PATH = IConstants.TESTPATH;
	public static long MOD = 100000000;
	public static String COMMON_ATTR = "Ti,CC,Id,AA.AuId,AA.AfId,J.JId,C.CId,F.FId,RId";
	public static final String keys[] = new String[]{
			"667a998acb5b4eae8a4e10fcdd1a00ff",
			"bd00351c15094393878d36b14742faea",
			"6cc3c947cc464929b4d12575fbc0a234",
			"10c7ebb073934d7e976cc555485f859c"};
	
	public static Map queryData(String expr, String attributes, String count, String offset) {
		long s = System.currentTimeMillis();

		String ttl = "evaluate";
		String url = "https://api.projectoxford.ai/academic/v1.0/evaluate";
		Map result = null;
		HttpClient httpclient = HttpClients.createDefault();
		
		int counter = COUNTER++;
		String key = keys[counter % keys.length];
		log.info("URL:" + url);
		log.info("expr:" + expr);
		log.info("count:" + count);
		log.info("key:" + key);
		log.info("offset:" + offset);
		try {
			URIBuilder builder = new URIBuilder(url);

			builder.setParameter("expr", expr);
			builder.setParameter("model", "latest");
			builder.setParameter("attributes", attributes);
			builder.setParameter("count", count);
			builder.setParameter("offset", offset);

			URI uri = builder.build();
			HttpGet request = new HttpGet(uri);
			request.setHeader("Ocp-Apim-Subscription-Key", key);

			HttpResponse response = httpclient.execute(request);
			HttpEntity entity = response.getEntity();

			if (entity != null) {
				String rs = EntityUtils.toString(entity);
				long l = System.currentTimeMillis();
				
				Map obj = JSON.parseObject(rs);
				result = obj;

				if (IConstants.LOG) {
					String fileName = String.format("%s/%04d_%s_%d.json", PATH, counter, ttl, l % MOD);
					log.debug("Writing to file: " + fileName);
					IOUtil.writeStringToFile(fileName, rs);
				} else {
					COUNTER++;
				}

			}
		} catch (Exception e) {
			log.error(e.getMessage());
		}
		return result;
	}
}
