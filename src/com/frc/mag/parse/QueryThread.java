package com.frc.mag.parse;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.frc.mag.send.BaseSender;

public class QueryThread extends Thread {
	public static final Logger log = LoggerFactory.getLogger("WF");
	public static final Logger FLOW = LoggerFactory.getLogger("FLOW");
	
	protected String cond;
	protected String attr;
	protected String count;
	protected String offset;
	protected Map result;
	
	public QueryThread(String cond, String attr, String count, String offset) {
		this.cond = cond;
		this.attr = attr;
		this.count = count;
		this.offset = offset;
	}
	
	@Override
	public void run() {
		int retry = 0;
		while (retry < IConstants.RETRY_TIME) {
			retry++;
			result = BaseSender.queryData(cond, attr, count, offset);
			if (result != null && !result.containsKey("error")) {
				break;
			}
			log.error("result is null or contains error:" + result);
		}
	}

	public Map getResult() {
		return result;
	}
}
