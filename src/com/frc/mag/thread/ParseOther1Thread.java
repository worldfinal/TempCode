package com.frc.mag.thread;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.frc.mag.bean.BigDataNode;
import com.frc.mag.bean.DataNode;
import com.frc.mag.parse.IConstants;

public class ParseOther1Thread extends WFThread {
	
	public Map<String, Boolean> dupMap1;
	
	public ParseOther1Thread(List<DataNode> otherData1, List<DataNode> otherData2, List<BigDataNode> idList1,
			List<DataNode> idList2, Object obj, long id, int rootType, int isStartPoint) {
		super(otherData1, otherData2, idList1, idList2, obj, id, rootType, isStartPoint);
	}
	
	@Override
	public void run() {
		dupMap1 = new HashMap<String, Boolean>();
		if (rootType == 0) {
			FLOW.debug("ParseOther1Thread [Id] start");
			parsePaperObject((Map)obj);
			FLOW.debug("ParseOther1Thread [Id] otherData1.size=", otherData1.size());
			FLOW.debug("ParseOther1Thread [Id] end");
		} else {
			FLOW.debug("ParseOther1Thread [AuId] start");
			List entityList = (List)obj;
			for (int i = 0; i < entityList.size(); i++) {
				parseAuObject((Map)entityList.get(i));
			}
			FLOW.debug("ParseOther1Thread [AuId] otherData1.size=", otherData1.size());
			FLOW.debug("ParseOther1Thread [AuId] start");
		}
		
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
