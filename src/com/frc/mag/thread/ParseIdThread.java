package com.frc.mag.thread;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.frc.mag.bean.DataNode;
import com.frc.mag.parse.IConstants;

public class ParseIdThread extends Thread {
	public static final Logger log = LoggerFactory.getLogger("WF");
	public static final Logger FLOW = LoggerFactory.getLogger("FLOW");
	public List<DataNode> otherData1; // Level1
	public Map<String, Boolean> dupMap1;
	public Map paper = null;
	public Map result = null;
	
	public ParseIdThread(Map paper, List<DataNode> otherData1) {
		this.paper = paper;
		this.otherData1 = otherData1;
		dupMap1 = new HashMap<String, Boolean>();
	}
	
	@Override
	public void run() {
		parsePaperObject(paper);
		
		List<String> auList = new ArrayList<String>();
		for (DataNode node : otherData1) {
			if (node.type == IConstants.SHORT_AUID) {
				auList.add(String.format("%d", node.val));
			}
		}
		QueryAfIdThread thread = new QueryAfIdThread(auList);
		thread.start();
		try {
			thread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		result = thread.result;
	}
	
	protected void parsePaperObject(Map object) {
		long id = toMyLong(object.get("Id"));

		if (object.containsKey("AA")) {
			List AA = (List) object.get("AA");
			if (AA != null) {
				for (int i = 0; i < AA.size(); i++) {
					Map data = (Map) AA.get(i);
					long auid = toMyLong(data.get("AuId"));
					insert(auid, IConstants.SHORT_AUID);
				}
			}
		}
		if (object.containsKey("F")) {
			List F = (List) object.get("F");
			if (F != null) {
				for (int i = 0; i < F.size(); i++) {
					Map data = (Map) F.get(i);
					long fid = toMyLong(data.get("FId"));

					insert(fid, IConstants.SHORT_AFID);
				}
			}
		}
		if (object.containsKey("C")) {
			Map C = (Map) object.get("C");
			if (C != null) {
				long cid = toMyLong(C.get("CId"));
				insert(cid, IConstants.SHORT_CID);
			}
		}
		if (object.containsKey("J")) {
			Map J = (Map) object.get("J");
			if (J != null) {
				long jid = toMyLong(J.get("JId"));
				insert(jid, IConstants.SHORT_JID);
			}
		}
	}
	private void insert(long val, short type) {
		DataNode node = new DataNode(val, type);
		String str = String.format("%d_%d", type, val);
		if (!dupMap1.containsKey(str)) {
			dupMap1.put(str, true);
			otherData1.add(node);
		}
	}
	private long toMyLong(Object obj) {
		if (obj instanceof Long) {
			return (long) obj;
		} else if (obj instanceof Integer) {
			return (long) (int) obj;
		} else if (obj instanceof String) {
			return Long.parseLong((String)obj);
		} else {
			log.error("Unknow type!" + obj);
			return 0;
		}
		
	}
}
