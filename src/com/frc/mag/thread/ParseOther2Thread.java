package com.frc.mag.thread;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.frc.mag.bean.BigDataNode;
import com.frc.mag.bean.DataNode;
import com.frc.mag.parse.IConstants;
import com.frc.mag.send.SenderImpl;

public class ParseOther2Thread extends WFThread {
	
	public Map<String, Boolean> dupMap1;
	
	public ParseOther2Thread(List<DataNode> otherData1, List<DataNode> otherData2, List<BigDataNode> idList1,
			List<DataNode> idList2, Object obj, long id, int rootType, int isStartPoint) {
		super(otherData1, otherData2, idList1, idList2, obj, id, rootType, isStartPoint);
	}
	
	@Override
	public void run() {
		Map<Long, Boolean> dumMap = new HashMap<Long, Boolean>();
		List<String> auList = new ArrayList<String>();
		for (int i = 0; i < otherData1.size(); i++) {
			DataNode node = otherData1.get(i);
			if (node.type == IConstants.SHORT_AUID) {
				String str = String.format("%d", node.val);
				auList.add(str);
				dumMap.put(node.val, true);
			}
		}
		SenderImpl sender = new SenderImpl();
		result = sender.getAfIdList(auList);
		List entities = (List)result.get("entities");
		log.debug("Before ParseOther2Thread, otherData2.size={}", otherData2.size());
		for (int i = 0; i < entities.size(); i++){ 
			Map ent = (Map)entities.get(i);
			if (ent.containsKey("AA")) {
				List AAlist = (List)ent.get("AA");
				for (int j = 0; j < AAlist.size(); j++) {
					Map AA = (Map)AAlist.get(j);
					if (AA.containsKey("AuId")) {
						long auid = toMyLong(AA.get("AuId"));
						if (dumMap.containsKey(auid)) {
							if (AA.containsKey("AfId")) {
								long afid = toMyLong(AA.get("AfId"));
								DataNode dataNode = new DataNode(afid, IConstants.SHORT_AFID, auid);
								otherData2.add(dataNode);
							}
						}
					}
				}
			}
		}
		log.debug("After ParseOther2Thread, otherData2.size={}", otherData2.size());
	}
	
	protected void parsePaperObject(Map object) {
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
	protected void parseAuObject(Map object) {
		if (object.containsKey("AA")) {
			List AA = (List) object.get("AA");
			if (AA != null) {
				for (int i = 0; i < AA.size(); i++) {
					Map data = (Map) AA.get(i);
					long auid = toMyLong(data.get("AuId"));
					if (auid == id) {
						if (data.containsKey("AfId")) {
							long afid = toMyLong(data.get("AfId"));
							insert(afid, IConstants.SHORT_AFID);
							break;
						}
					}					
				}
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
}
