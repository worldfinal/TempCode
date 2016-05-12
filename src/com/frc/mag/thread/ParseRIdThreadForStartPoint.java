package com.frc.mag.thread;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.frc.mag.parse.IConstants;

public class ParseRIdThreadForStartPoint extends Thread {
	public static final Logger log = LoggerFactory.getLogger("WF");
	public static final Logger FLOW = LoggerFactory.getLogger("FLOW");

	public Map paper = null;
	public Map result = null;

	public ParseRIdThreadForStartPoint(Map paper) {
		this.paper = paper;
	}

	@Override
	public void run() {
		result = new HashMap();
		List ridArr = (List) paper.get("RId");

		if (ridArr == null || ridArr.size() == 0) {
			return;
		}
		String cond = "";
//		cond = "Id=" + ridArr.get(0);
		List<QueryThread> threadList = new ArrayList<QueryThread>();
		log.info("ParseRIdThread::RidArr.size={}", ridArr.size());
		
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
			log.debug("ParseRIdThread::sleep 500ms");
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
		
		List entities = new ArrayList();
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
