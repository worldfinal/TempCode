package com.frc.mag.thread;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.frc.mag.bean.BigDataNode;
import com.frc.mag.bean.DataNode;
import com.frc.mag.parse.IConstants;

public class ParseIdList1ForStartPoint extends WFThread {
	public Map<String, Boolean> dupMap;
	public Map<String, Boolean> idDupMap;
	public List entities = null;
	
	public ParseIdList1ForStartPoint(List<DataNode> otherData1, List<DataNode> otherData2, List<BigDataNode> idList1,
			List<DataNode> idList2, Object obj, long id, int rootType, int isStartPoint) {
		super(otherData1, otherData2, idList1, idList2, obj, id, rootType, isStartPoint);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public void run() {
		dupMap = new HashMap<String, Boolean>();
		idDupMap = new HashMap<String, Boolean>();
		result = new HashMap();
		if (rootType == 0) {
			if (isStartPoint == 0) {
				queryIdFromId();
				parseIdFromId();	//idList1,otherData2,idList2
			} else {
				queryIdToId();
				parseIdFromId();	//idList1,otherData2,idList2
			}
		} else {
			parseIdFromAuId();	//idList1,otherData2,idList2
		}
	}

	protected void parseIdFromId() {
		if (entities == null) {
			log.warn("[parseIdFromId] entities is null!");
			return;
		}
		for (int i = 0; i < entities.size(); i++) {
			Map paper = (Map)entities.get(i);
			BigDataNode bdn = createBDN(paper);
			idList1.add(bdn);
			parsePaperObject(paper, bdn.val);
			
			for (long l : bdn.ridList) {
				long fromId = toMyLong(paper.get("Id"));
				String key = String.format("%d_%d", l, fromId);
				if (!idDupMap.containsKey(key)) {
					DataNode node = new DataNode(l, IConstants.SHORT_ID, fromId);
					idList2.add(node);
					idDupMap.put(key, true);
				}
			}
		}
	}
	protected void parseIdFromAuId() {
		List entityList = (List)obj;
		
		for (int i = 0; i < entityList.size(); i++) {
			Map paper = (Map)entityList.get(i);
			BigDataNode bdn = createBDN(paper);
			idList1.add(bdn);
			
			parsePaperObject(paper, bdn.val);
			
			for (long l : bdn.ridList) {
				String key = String.format("%d_%d", l, bdn.val);
				if (!idDupMap.containsKey(key)) {
					DataNode node = new DataNode(l, IConstants.SHORT_ID, bdn.val);
					idList2.add(node);
					idDupMap.put(key, true);
				}
			}
		}
	}

	private void insert(long val, short type, long from) {
		DataNode node = new DataNode(val, type, from);
		String str = String.format("%d_%d_%d", type, val, from);
		if (!dupMap.containsKey(str)) {
			dupMap.put(str, true);
			otherData2.add(node);
		}
	}
	protected void parsePaperObject(Map object, long from) {
		if (object.containsKey("AA")) {
			List AA = (List) object.get("AA");
			if (AA != null) {
				for (int i = 0; i < AA.size(); i++) {
					Map data = (Map) AA.get(i);
					long auid = toMyLong(data.get("AuId"));
					insert(auid, IConstants.SHORT_AUID, from);
				}
			}
		}
		if (object.containsKey("F")) {
			List F = (List) object.get("F");
			if (F != null) {
				for (int i = 0; i < F.size(); i++) {
					Map data = (Map) F.get(i);
					long fid = toMyLong(data.get("FId"));

					insert(fid, IConstants.SHORT_AFID, from);
				}
			}
		}
		if (object.containsKey("C")) {
			Map C = (Map) object.get("C");
			if (C != null) {
				long cid = toMyLong(C.get("CId"));
				insert(cid, IConstants.SHORT_CID, from);
			}
		}
		if (object.containsKey("J")) {
			Map J = (Map) object.get("J");
			if (J != null) {
				long jid = toMyLong(J.get("JId"));
				insert(jid, IConstants.SHORT_JID, from);
			}
		}
	}
	protected void queryIdToId() {
		String cond = String.format("RId=%d", id);
		List<QueryThread> threadList = new ArrayList<QueryThread>();
		
		for (int i = 0; i < 3; i++) {
			String offset = String.format("%d", i * IConstants.D_MAX_COUNT);
			QueryThread thread = new QueryThread(cond, IConstants.COMMON_ATTR, IConstants.MAX_COUNT, offset);
			thread.start();
			threadList.add(thread);
		}
		
		try {
			for (int i = 0; i < threadList.size(); i++) {
				QueryThread thread = threadList.get(i);
				thread.join();
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		entities = new ArrayList();
		for (int i = 0; i < threadList.size(); i++) {
			QueryThread thread = threadList.get(i);
			Map rs = thread.result;
			List list = (List)rs.get("entities");
			entities.addAll(list);
			log.info("ParseRIdThread:: rs size={}", list.size());
		}
		result.put("entities", entities);
		log.info("ParseRIdThread:: total result size={}", entities.size());
	}
	protected void queryIdFromId() {
		Map paper = (Map)obj;
		List ridArr = (List) paper.get("RId");
		if (ridArr == null || ridArr.size() == 0) {
			return;
		}
		String cond = "";
		List<QueryThread> threadList = new ArrayList<QueryThread>();
		log.info("queryIdFromId::RidArr.size={}", ridArr.size());
		
		int k = 0;
		for (k = 0; k < ridArr.size();) {
			cond = "Id=" + ridArr.get(k++);
			for (int j = 1; j < 50 && k < ridArr.size(); j++) {
				String str = String.format("Or(%s,Id=%s)", cond, ridArr.get(k));
				k++;
				cond = str;	
			}	
			QueryThread thread = new QueryThread(cond, IConstants.COMMON_ATTR, IConstants.MAX_COUNT, "0");
			thread.start();
			threadList.add(thread);
			log.debug("queryIdFromId::sleep 500ms");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
	
		try {
			for (int i = 0; i < threadList.size(); i++) {
				QueryThread thread = threadList.get(i);
				thread.join();
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		entities = new ArrayList();
		for (int i = 0; i < threadList.size(); i++) {
			QueryThread thread = threadList.get(i);
			Map rs = thread.result;
			List list = (List)rs.get("entities");
			entities.addAll(list);
			log.info("ParseRIdThread:: rs size={}", list.size());
		}
		result.put("entities", entities);
		log.info("ParseRIdThread:: total result size={}", entities.size());
	}
}
