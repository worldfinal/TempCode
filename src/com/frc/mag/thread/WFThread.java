package com.frc.mag.thread;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.frc.mag.bean.BigDataNode;
import com.frc.mag.bean.DataNode;

public class WFThread extends Thread {
	public static final Logger log = LoggerFactory.getLogger("WF");
	public static final Logger FLOW = LoggerFactory.getLogger("FLOW");
	
	public Object obj = null;
	public Map result;
	
	public List<DataNode> otherData1; // Level1
	public List<DataNode> otherData2; // Level2
	public List<BigDataNode> idList1;
	public List<DataNode> idList2;
	
	public long id;
	public int rootType; // 0-Id, 1-AuId
	public int isStartPoint;// 0-start, 1-end
	
	public WFThread(List<DataNode> otherData1, List<DataNode> otherData2,
			List<BigDataNode> idList1, List<DataNode> idList2,
			Object obj, long id, int rootType, int isStartPoint) {
		this.otherData1 = otherData1;
		this.otherData2 = otherData2;
		this.idList1 = idList1;
		this.idList2 = idList2;
		this.obj = obj;
		this.id = id;
		this.rootType = rootType;
		this.isStartPoint = isStartPoint;
	}
	
	protected long toMyLong(Object obj) {
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
	protected BigDataNode createBDN(Map paper) {
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
}
