package com.frc.mag.parse;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.frc.mag.bean.BigDataNode;
import com.frc.mag.bean.DataNode;
import com.frc.mag.send.BaseSender;
import com.frc.mag.send.SenderImpl;
import com.frc.mag.thread.ParseIdList1ForStartPoint;
import com.frc.mag.thread.ParseIdThread;
import com.frc.mag.thread.ParseOther1Thread;
import com.frc.mag.thread.ParseOther2Thread;
import com.frc.mag.thread.ParseRIdThreadForStartPoint;
import com.frc.mag.thread.QueryThread;
import com.frc.mag.thread.WFThread;
import com.frc.util.IOUtil;

@SuppressWarnings("rawtypes")
public class Processor extends Thread {
	private static final Logger log = LoggerFactory.getLogger("WF");
	private static final Logger FLOW = LoggerFactory.getLogger("FLOW");
	
	private static int COUNTER = 1;
	
	protected static SenderImpl sender = new SenderImpl();

	public Map<String, Boolean> dupMap1;
	public Map<String, Boolean> dupMap2;
	public Map<Long, List> ridMap;
	// public List<DataNode> listData;

	public long id; // It maybe "Id" or "AA.AuId"
	public int rootType; // 0-Id, 1-AuId
	public int isStartPoint;// 0-start, 1-end

	// Id->FId,CId,JId,AuId
	// AuId->AfId
	public List<DataNode> otherData1; // Level1
	public List<DataNode> otherData2; // Level2
	public Map<String, Boolean> otherMapData1;
	public Map<String, Boolean> otherMapData2;

	// Id->Id
	// AuId->Id
	public List<BigDataNode> idList1;
	public List<DataNode> idList2;

	public static String PATH = IConstants.TESTPATH;
	public static long MOD = 100000000;
//	public static String COMMON_ATTR = "CC,Id,AA.AuId,AA.AfId,J.JId,C.CId,F.FId,RId";

	// For testing only
	public static long ttl_query_use_time;
	
	public WFThread other1Thread = null;
	public WFThread other2Thread = null;
	public WFThread idList1Thread = null;
	public WFThread idList2Thread = null;

	public void init() {
		// listData = new ArrayList<DataNode>();
		dupMap1 = new HashMap<String, Boolean>();
		dupMap2 = new HashMap<String, Boolean>();
		ridMap = new HashMap<Long, List>();

		otherData1 = new ArrayList<DataNode>();
		otherData2 = new ArrayList<DataNode>();
		idList1 = new ArrayList<BigDataNode>();
		idList2 = new ArrayList<DataNode>();

		ttl_query_use_time = 0;

	}

	public void parseJsonObject(Map object) {
		List array = (List) object.get("entities");
		if (array != null) {
			log.debug("entities.size=" + array.size());
			if (array.size() > 1) {
				FLOW.info("{} is AuId", id);
				// Level 0 : AuId
				rootType = 1;
				other1Thread = new ParseOther1Thread(otherData1, otherData2, idList1, idList2, array, id, rootType, isStartPoint);
				idList1Thread = new ParseIdList1ForStartPoint(otherData1, otherData2, idList1, idList2, array, id, rootType, isStartPoint);
			
				other1Thread.start();
				idList1Thread.start();
				
				try {
					other1Thread.join();
					idList1Thread.join();
				} catch (InterruptedException e) {					
				}
				
//				processAuId(array);
			} else if (array.size() == 1) {
				// Level 0 : Id
				rootType = 0;
				FLOW.info("{} is PaperId", id);
				other1Thread = new ParseOther1Thread(otherData1, otherData2, idList1, idList2, array.get(0), id, rootType, isStartPoint);
				idList1Thread = new ParseIdList1ForStartPoint(otherData1, otherData2, idList1, idList2, array.get(0), id, rootType, isStartPoint);
				other2Thread = new ParseOther2Thread(otherData1, otherData2, idList1, idList2, array.get(0), id, rootType, isStartPoint);
				
				other1Thread.start();
				idList1Thread.start();
				
				try {
					other1Thread.join();
					idList1Thread.join();
				} catch (InterruptedException e) {					
				}
				
				if (isStartPoint == 0) {
					other2Thread.start();
					try {
						other2Thread.join();
					} catch (InterruptedException e) {
					}
				}				
//				processPaper((Map) array.get(0));
			}
		}
	}

	public void insert(long val, short type) {
		DataNode node = new DataNode(val, type);
		String str = String.format("%d_%d", type, val);
		if (!dupMap1.containsKey(str)) {
			dupMap1.put(str, true);
			otherData1.add(node);
		}
	}

	public void insertLevel2(long val, short type, long from) {
		DataNode node = new DataNode(val, type, from);
		String str = String.format("%d_%d_%d", type, val, from);
		if (!dupMap2.containsKey(str)) {
			dupMap2.put(str, true);
			otherData2.add(node);
		}
	}

	

	@SuppressWarnings("unchecked")
	public void seeHowManyRid() {
		List<Long> ridList = new ArrayList<Long>();
		for (int i = 0; i < idList1.size(); i++) {
			BigDataNode bdn = idList1.get(i);
			long val = bdn.val;
			for (int j = 0; j < bdn.ridList.size(); j++) {
				long r = bdn.ridList.get(j);
				ridList.add(r);
				Object obj = ridMap.get(r);
				if (obj == null) {
					List arr = new ArrayList();
					arr.add(val);
					ridMap.put(r, arr);
				} else {
					List arr = (List) obj;
					arr.add(val);
				}
			}
		}
		Collections.sort(ridList);
		int m = 1;
		for (int i = 1; i < ridList.size(); i++) {
			if (ridList.get(i).longValue() != ridList.get(i - 1).longValue()) {
				m++;
			}
		}
		String msg = String.format("Total RId:%d\nDistinct RId:%d\n", ridList.size(), m);
		log.debug(msg);
		Comparator c = new Comparator<DataNode>() {
			@Override
			public int compare(DataNode arg0, DataNode arg1) {
				if (arg0.val != arg1.val) {
					return arg0.val > arg1.val ? 1 : -1;
				} else if (arg0.type != arg1.type) {
					return arg0.type > arg1.type ? 1 : -1;
				} else if (arg0.from != arg1.from) {
					return arg0.from > arg1.from ? 1 : -1;
				} else {
					return 0;
				}
			}
		};
		Collections.sort(otherData1, c);
		Collections.sort(otherData2, c);

		Comparator c2 = new Comparator<BigDataNode>() {
			@Override
			public int compare(BigDataNode x, BigDataNode y) {
				if (x.val != y.val) {
					return x.val > y.val ? 1 : -1;
				} else {
					return 0;
				}
			}

		};
		Collections.sort(idList1, c2);
	}
	@Override
	public void run() {
		String expr = String.format("Or(Id=%d,Composite(AA.AuId=%d))", id, id);
		Map obj = BaseSender.queryData(expr, IConstants.COMMON_ATTR, IConstants.MAX_COUNT, "0");
//		Map obj = queryData(expr, COMMON_ATTR, IConstants.MAX_COUNT);
		parseJsonObject(obj);
		
		after();
	}

	public void processAuId(List array) {
		for (int i = 1; i < array.size(); i++) {
			Map paper = (Map) array.get(i);
			// Insert AuId->AfId
			if (paper.containsKey("AA")) {
				List AA = (List) paper.get("AA");
				parseAuIdWithAA(AA);
			}
			// Insert AuId->Id
			BigDataNode bdn = createBDN(paper);
			idList1.add(bdn);
		}
	}

	public void processPaper(Map paper) {
		long begin = System.currentTimeMillis();
		// Insert Id->FId,CId,JId,AuId
		// Level 1: other
//		parsePaperObject(paper);
		ParseIdThread parseIdThread = new ParseIdThread(paper, otherData1);
		parseIdThread.start();

		// Inser Id--Rid-->Id
		// Level 1 : Id
		ParseRIdThreadForStartPoint parseRIdThread = new ParseRIdThreadForStartPoint(paper);
		parseRIdThread.start();

		try {
			parseRIdThread.join();
			parseIdThread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Map result = queryData(cond, COMMON_ATTR, IConstants.MAX_COUNT);
		Map result = parseRIdThread.result;
		Map result2 = parseIdThread.result;

		printTime(begin, "Parse Id(L1)");

		begin = System.currentTimeMillis();
		// Level 2, Id->Other
		parsePaperId(result);
		
		String fileName = String.format("%s/%04d_RID_%d.json", PATH, COUNTER++, begin % MOD);
		IOUtil.writeStringToFile(fileName, JSONObject.toJSONString(result));		

		// Level 2, Au->Af

		printTime(begin, "Parse Id(L2)");
	}

	public void parsePaperId(Map object) {
		if (!object.containsKey("entities")) {
			return;
		}
		List array = (List) object.get("entities");
		if (array != null) {
			for (int i = 0; i < array.size(); i++) {
				Map paper = (Map) array.get(i);
				BigDataNode bdn = createBDN(paper);
				idList1.add(bdn);

				long paperId = toMyLong(paper.get("Id"));
				processLevel2Paper(paper, paperId);
			}
		}
	}

	public void processLevel2Paper(Map paper, long paperId) {
		long val;
		if (paper.containsKey("AA")) {
			List AA = (List) paper.get("AA");
			for (int i = 0; i < AA.size(); i++) {
				Map aa = (Map) AA.get(i);
				if (aa.containsKey("AuId")) {
					val = toMyLong(aa.get("AuId"));
					insertLevel2(val, IConstants.SHORT_AUID, paperId);
				}
			}
		}
		if (paper.containsKey("F")) {
			List F = (List) paper.get("F");
			for (int i = 0; i < F.size(); i++) {
				Map f = (Map) F.get(i);
				if (f.containsKey("FId")) {
					val = toMyLong(f.get("FId"));
					insertLevel2(val, IConstants.SHORT_FID, paperId);
				}
			}
		}
		if (paper.containsKey("C")) {
			Map C = (Map) paper.get("C");
			val = toMyLong(C.get("CId"));
			insertLevel2(val, IConstants.SHORT_CID, paperId);
		}
		if (paper.containsKey("J")) {
			Map J = (Map) paper.get("J");
			val = toMyLong(J.get("JId"));
			insertLevel2(val, IConstants.SHORT_JID, paperId);
		}
	}

	public BigDataNode createBDN(Map paper) {
		BigDataNode bdn = new BigDataNode();
		bdn.val = toMyLong(paper.get("Id"));
		List<Long> list = new ArrayList<Long>();
		if (paper.containsKey("RId")) {
			List ridArr = (List) paper.get("RId");
			for (int i = 0; i < ridArr.size(); i++) {
				long v = toMyLong(ridArr.get(i));
				list.add(v);
			}
		}
		bdn.ridList = list;
		return bdn;
	}
	public void parseAuIdWithAA(List arr) {
		for (int i = 0; i < arr.size(); i++) {
			Map obj = (Map) arr.get(i);
			if (!obj.containsKey("AuId")) {
				continue;
			}
			Object lvl = obj.get("AuId");
			long auid = toMyLong(lvl);

			if (auid == id) {
				if (obj.containsKey("AfId")) {
					insert(toMyLong(obj.get("AFId")), IConstants.SHORT_AFID);
				}
				break;
			}
		}
	}

	public void parsePaperObject(Map object) {
		long id = toMyLong(object.get("Id"));

		if (object.containsKey("AA")) {
			List AA = (List) object.get("AA");
			if (AA != null) {
				parseAAObject(id, AA);
			}
		}
		if (object.containsKey("F")) {
			List F = (List) object.get("F");
			if (F != null) {
				parseFObject(id, F);
			}
		}
		if (object.containsKey("C")) {
			Map C = (Map) object.get("C");
			if (C != null) {
				parseCObject(id, C);
			}
		}
		if (object.containsKey("J")) {
			Map J = (Map) object.get("J");
			if (J != null) {
				parseJObject(id, J);
			}
		}
	}

	public void parseAAObject(long id, List arr) {
		log.debug("AA.size=" + arr.size());
		for (int i = 0; i < arr.size(); i++) {
			Map data = (Map) arr.get(i);
			long auid = toMyLong(data.get("AuId"));

			insert(auid, IConstants.SHORT_AUID);
		}
	}

	public void parseFObject(long id, List arr) {
		for (int i = 0; i < arr.size(); i++) {
			Map data = (Map) arr.get(i);
			long fid = toMyLong(data.get("FId"));

			insert(fid, IConstants.SHORT_AFID);
		}
	}

	public void parseCObject(long id, Map object) {
		long cid = toMyLong(object.get("CId"));

		insert(cid, IConstants.SHORT_CID);
	}

	public void parseJObject(long id, Map object) {
		long jid = toMyLong(object.get("JId"));

		insert(jid, IConstants.SHORT_JID);
	}

	public static Map queryData11(String expr, String attributes, String count) {
		long s = System.currentTimeMillis();

		String ttl = "evaluate";
		String url = "https://api.projectoxford.ai/academic/v1.0/" + ttl;
		Map result = null;
		HttpClient httpclient = HttpClients.createDefault();
		log.info("URL:" + url);
		log.info("expr:" + expr);
		log.info("count:" + count);
		try {
			URIBuilder builder = new URIBuilder(url);

			builder.setParameter("expr", expr);
			builder.setParameter("model", "latest");
			builder.setParameter("attributes", attributes);
			builder.setParameter("count", count);
			builder.setParameter("offset", "0");

			URI uri = builder.build();
			HttpGet request = new HttpGet(uri);
			request.setHeader("Ocp-Apim-Subscription-Key", "8e95ffbbfb294ed78330169af03ad99c");

			HttpResponse response = httpclient.execute(request);
			HttpEntity entity = response.getEntity();

			if (entity != null) {
				String rs = EntityUtils.toString(entity);
				// System.out.println(rs);
				long l = System.currentTimeMillis();
				String fileName = String.format("%s/%04d_%s_%d.json", PATH, COUNTER++, ttl, l % MOD);
				log.debug("Writing to file: " + fileName);

				Map obj = JSON.parseObject(rs);
				result = obj;

				IOUtil.writeStringToFile(fileName, rs);

			}
		} catch (Exception e) {
			log.debug(e.getMessage());
		}
		printTime(s, "+++++Query");
		ttl_query_use_time += (System.currentTimeMillis() - s);
		return result;
	}

	private long toMyLong(Object obj) {
		if (obj instanceof Long) {
			return (long) obj;
		} else if (obj instanceof Integer) {
			return (long) (int) obj;
		}
		return 0;
	}

	private static void printTime(long begin, String ttl) {
		long end = System.currentTimeMillis();
		String msg = String.format("[%s] Use time: %d (ms)", ttl, end - begin);
		log.info(msg);
	}
	@Before
	public void before() {
		init();
	}

	@After
	public void after() {
		log.debug("=== Level 1 =====");
		log.debug("idList.size:" + idList1.size());
		log.debug("otherData1.size:" + otherData1.size());

		log.debug("=== Level 2 =====");
		log.debug("otherData2.size:" + otherData2.size());

		log.debug("=== other =====");
		seeHowManyRid();
		log.debug("ridMap.size:" + ridMap.size());
	}
}
