package com.frc.mag.thread;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.frc.mag.parse.IConstants;

public class ParseRIdThread extends Thread {
	public static final Logger log = LoggerFactory.getLogger("WF");
	public static final Logger FLOW = LoggerFactory.getLogger("FLOW");

	public Map paper = null;
	public Map result = null;

	public ParseRIdThread(Map paper) {
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
		cond = "RId=" + ridArr.get(0);
		for (int i = 1; i < ridArr.size() && i < 70; i++) {
			String str = String.format("Or(%s,RId=%s)", cond, ridArr.get(i));
			cond = str;
		}

		List<QueryThread> threadList = new ArrayList<QueryThread>();
		for (int i = 0; i < 3; i++) {
			QueryThread thread = new QueryThread(cond, IConstants.COMMON_ATTR, IConstants.MAX_COUNT,
					String.format("%d", i * IConstants.D_MAX_COUNT));
			thread.start();
			threadList.add(thread);
			log.debug("ParseRIdThread::sleep 500ms");
			try {
				Thread.sleep(500);
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
		
		for (int i = 0; i < threadList.size(); i++) {
			QueryThread thread = threadList.get(i);
			Map rs = thread.result;
			result.putAll(rs);
		}
		
	}
}
