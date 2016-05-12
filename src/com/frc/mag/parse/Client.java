package com.frc.mag.parse;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.frc.mag.bean.BigDataNode;
import com.frc.mag.bean.DataNode;
import com.frc.util.IOUtil;

public class Client {
	private Logger log = LoggerFactory.getLogger("WF");
	private static final Logger FLOW = LoggerFactory.getLogger("FLOW");
	
	public Processor p1;
	public Processor p2;
	public List<List> result;
	
	/*
	 * Uri:/mag?id2=2180737804&id1=2251253715
Done

Uri:/mag?id2=189831743&id1=2147152072
Done

Uri:/mag?id2=2310280492&id1=2332023333
*/

	@Before
	public void init() {
		p1 = new Processor();
		p2 = new Processor();
		p1.init();
		p2.init();
		result = new ArrayList<List>();
	}
	
	public String work(String id1, String id2) {
		log.info("=====================================================\n[Input Data] id1={},id2={}", id1, id2);
		
		long s = System.currentTimeMillis();
		
		init();
		p1.id = toMyLong(id1);
		p2.id = toMyLong(id2);
		
		FLOW.info("=====================================================\n[Input Data] id1={},id2={}", p1.id, p2.id);
		try {
			p1.start();
			p2.start();
			p1.join();
			p2.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		log.info("!!!!Start final Handle!!!");
		long s1 = System.currentTimeMillis();
		finalHandle();
		printTime(s1, "Process use time");
		printTime(s, "Total use time");
		
		String rs = JSON.toJSONString(result);
		long end = System.currentTimeMillis();
		FLOW.info("[Result][{} ms]:{}\n", (end-s), rs);
		return rs;
	}

	@Test
	public void testMap() {
		long data[][] = new long[][] { { 189831743L, 2251253715L }, { 2166251959L, 2107262287L } };
		long s = System.currentTimeMillis();
		int idx = 0;
		p1.id = data[idx][0];
		p2.id = data[idx][1];
		p1.isStartPoint = 0;
		p2.isStartPoint = 1;
		// p1.test();
		// p2.test();
		try {
			p1.start();
			p2.start();
			p1.join();
			p2.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		log.info("!!!!Start final Handle!!!");
		long s1 = System.currentTimeMillis();
		finalHandle();
		printTime(s1, "Process use time");
		long queryTime = p1.ttl_query_use_time + p2.ttl_query_use_time;

		printTime(s, "Total use time");
	}

	public void finalHandle() {
		log.info(String.format("P1[%d,%d] P2[%d,%d]", p1.id, p1.rootType, p2.id, p2.rootType));
		handleHop1();
		handleHop2();
		handleHop3();
		log.info("Result:" + result.size());
		if (result.size() < 2000) {
			String str = JSONObject.toJSONString(result);
			long l = System.currentTimeMillis();
			String path = String.format("%s/Result_%d.json", IConstants.TESTPATH, l % 10000000);
			IOUtil.writeStringToFile(path, str);
		}
		
		System.out.println("Done");
	}

	protected void handleHop1() {
		log.info("======handleHop1=========");
		// Id->Id
		if (p1.rootType == 0 && p2.rootType == 0) {
			List<BigDataNode> list = p1.idList1;
			log.info("====Id->Id");
			for (BigDataNode node : list) {
				if (node.val == p2.id) {
					List<Long> arr = new ArrayList<Long>();
					arr.add(p1.id);
					arr.add(p2.id);
					result.add(arr);
					log.debug(String.format("%d %d", p1.id, p2.id));
				}
			}
		}
		// Id->AuId
		if (p1.rootType == 0 && p2.rootType == 1) {
			log.info("====Id->AuId");
			List<DataNode> list = p1.otherData1;
			for (DataNode node : list) {
				if (node.type == IConstants.SHORT_AUID && node.val == p2.id) {
					List<Long> arr = new ArrayList<Long>();
					arr.add(p1.id);
					arr.add(p2.id);
					result.add(arr);
					log.debug(String.format("%d %d", p1.id, p2.id));
				}
			}
		}
		// AuId->Id
		if (p2.rootType == 0 && p1.rootType == 1) {
			log.info("====AuId->Id");
			List<DataNode> list = p2.otherData1;
			for (DataNode node : list) {
				if (node.type == IConstants.SHORT_AUID && node.val == p1.id) {
					List<Long> arr = new ArrayList<Long>();
					arr.add(p1.id);
					arr.add(p2.id);
					result.add(arr);
					log.debug(String.format("%d %d", p1.id, p2.id));
				}
			}
		}
	}

	protected void handleHop2() {
		log.info("======handleHop2=========");
		int i, j;
		// Id->other->Id
		if (p1.rootType == 0 && p2.rootType == 0) {
			i = j = 0;
			log.info("====Id->other->Id");
			while (i < p1.otherData1.size() && j < p2.otherData1.size()) {
				DataNode n1 = p1.otherData1.get(i);
				DataNode n2 = p2.otherData1.get(j);
				int k = compareDataNode(n1, n2);
				if (k == 0) {
					List<Long> arr = new ArrayList<Long>();
					arr.add(p1.id);
					arr.add(n1.val);
					arr.add(p2.id);
					result.add(arr);
					i++;
					j++;
					log.debug(String.format("%d %d[%d] %d", p1.id, n1.val, n1.type, p2.id));
				} else if (k == -1) {
					j++;
				} else {
					i++;
				}
			}
		}
		// Id->Id->Id
		if (p1.rootType == 0 && p2.rootType == 0) {
			i = j = 0;
			log.info("====Id->Id->Id");
			while (i < p1.idList1.size() && j < p2.idList1.size()) {
				BigDataNode n1 = p1.idList1.get(i);
				BigDataNode n2 = p2.idList1.get(j);
				if (n1.val == n2.val) {
					List<Long> arr = new ArrayList<Long>();
					arr.add(p1.id);
					arr.add(n1.val);
					arr.add(p2.id);
					result.add(arr);
					i++;
					j++;
					log.debug(String.format("%d %d %d", p1.id, n1.val, p2.id));
				} else if (n1.val > n2.val) {
					i++;
				} else {
					j++;
				}
			}
		}
		// Id->Id->AuId
		if (p1.rootType == 0 && p2.rootType == 1) {
			log.info("====Id->Id->AuId");
			for (i = 0; i < p1.otherData2.size(); i++) {
				DataNode node = p1.otherData2.get(i);
				if (node.val == p2.id && node.type == IConstants.SHORT_AUID) {
					List<Long> arr = new ArrayList<Long>();
					arr.add(p1.id);
					arr.add(node.from);
					arr.add(p2.id);
					result.add(arr);
					log.debug(String.format("%d %d %d", p1.id, node.from, p2.id));
				}
			}
		}
		// AuId->Id->Id
		if (p1.rootType == 1 && p2.rootType == 0) {
			log.info("====AuId->Id->Id");
			for (i = 0; i < p2.otherData2.size(); i++) {
				DataNode node = p2.otherData2.get(i);
				if (node.val == p1.id && node.type == IConstants.SHORT_AUID) {
					List<Long> arr = new ArrayList<Long>();
					arr.add(p1.id);
					arr.add(node.from);
					arr.add(p2.id);
					result.add(arr);
					log.debug(String.format("%d %d %d", p1.id, node.from, p2.id));
				}
			}
		}
		// Id->AfId->AuId
		// AfId->AuId->Id
		if (p1.rootType != p2.rootType) {
			i = j = 0;
			log.info("====Id->AfId->AuId");
			log.info("====AfId->AuId->Id");
			while (i < p1.otherData1.size() && j < p2.otherData1.size()) {
				DataNode n1 = p1.otherData1.get(i);
				DataNode n2 = p2.otherData1.get(j);
				if (n1.type != IConstants.SHORT_AFID || n2.type != IConstants.SHORT_AFID) {
					i++;
					j++;
					continue;
				}
				int k = compareDataNode(n1, n2);
				if (k == 0) {
					List<Long> arr = new ArrayList<Long>();
					arr.add(p1.id);
					arr.add(n1.val);
					arr.add(p2.id);
					result.add(arr);
					i++;
					j++;
					log.debug(String.format("%d %d %d", p1.id, n1.val, p2.id));
				} else if (k == -1) {
					j++;
				} else {
					i++;
				}
			}
		}
	}

	protected void handleHop3() {
		// Only care the middle 2 node
		int i, j;
		// Id-Id
		log.info("======== ?-Id-Id-?");
		List<Long> longList2 = new ArrayList<Long>();
		for (i = 0; i < p2.idList1.size(); i++) {
			longList2.add(p2.idList1.get(i).val);
		}
		Collections.sort(longList2);
		Collections.sort(p1.idList2, new Comparator<DataNode>(){
			@Override
			public int compare(DataNode x, DataNode y) {
				return x.val > y.val ? 1 : -1;
			}			
		});
		i = j = 0;
		while (i < p1.idList2.size() && j < longList2.size()) {
			DataNode node = p1.idList2.get(i);
			long y = longList2.get(j);
			if (y == node.val) {
				List<Long> arr = new ArrayList<Long>();
				arr.add(p1.id);
				arr.add(node.from);
				arr.add(node.val);
				arr.add(p2.id);
				result.add(arr);
				log.debug(String.format("%d %d %d %d", p1.id, node.from, node.val, p2.id));
				j++;
			} else if (y > node.val) {
				j++;
			} else {
				i++;
			}
		}
		
		// Id-other
		log.info("======== ?-Id-Other-?");
		i = 0;
		j = 0;
		while (i < p1.otherData2.size() && j < p2.otherData1.size()) {
			DataNode n1 = p1.otherData2.get(i);
			DataNode n2 = p2.otherData1.get(j);
			if (n1.val == n2.val) {
				List<Long> arr = new ArrayList<Long>();
				arr.add(p1.id);
				arr.add(n1.from);
				arr.add(n1.val);
				arr.add(p2.id);
				result.add(arr);
				i++;
				j++;
				log.debug(String.format("%d %d %d[%d] %d", p1.id, n1.from, n1.val, n2.type, p2.id));
			} else if (n1.val > n2.val) {
				i++;
			} else {
				j++;
			}
		}
		// Other-Id
		i = 0;
		j = 0;
		log.info("======== ?-Other-Id-?");
		while (i < p1.otherData1.size() && j < p2.otherData2.size()) {
			DataNode n1 = p1.otherData1.get(i);
			DataNode n2 = p2.otherData2.get(j);
			if (n1.val == n2.val) {
				List<Long> arr = new ArrayList<Long>();
				arr.add(p1.id);
				arr.add(n2.val);
				arr.add(n2.from);
				arr.add(p2.id);
				result.add(arr);
				i++;
				j++;
				log.debug(String.format("%d %d[%d] %d %d", p1.id, n2.val, n2.type, n2.from, p2.id));
			} else if (n1.val > n2.val) {
				i++;
			} else {
				j++;
			}
		}
	}

	public void test() {
		long begin = System.currentTimeMillis();
		String fileContent = "";
		String filePath = String.format("%s\\\\data_evaluate_22258223.json", IConstants.TESTPATH);
		URL conf = Thread.currentThread().getContextClassLoader().getResource(filePath);
		File file = new File(filePath);

		fileContent = IOUtil.readTxtFile(filePath);
		printTime(begin, "Read file");

		begin = System.currentTimeMillis();
		Map data = JSON.parseObject(fileContent);
		log.info("{}", data.get("expr"));
		List entities = (List) data.get("entities");
		Map paper = (Map) entities.get(0);
		long l = (long) paper.get("Id");
		log.info("l = " + l);

		// String txt = IOUtil.readTxtFile(filePath);

		printTime(begin, "ParseObject");

	}

	private void printTime(long begin, String ttl) {
		long end = System.currentTimeMillis();
		String msg = String.format("[%s] Use time: %d (ms)", ttl, end - begin);
		log.info(msg);
	}

	private int compareDataNode(DataNode arg0, DataNode arg1) {
		if (arg0.type == arg1.type && arg0.val == arg1.val) {
			return 0;
		}
		if (arg0.val != arg1.val) {
			return arg0.val < arg1.val ? 1 : -1;
		} else {
			return arg0.type < arg1.type ? 1 : -1;
		}
	}

	private long toMyLong(Object obj) {
		if (obj instanceof Long) {
			return (long) obj;
		} else if (obj instanceof Integer) {
			return (long) (int) obj;
		} else if (obj instanceof String) {
			return Long.parseLong((String)obj);
		}
		return 0;
	}
}
