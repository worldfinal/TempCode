package com.frc.mag.thread;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.frc.mag.send.SenderImpl;

public class QueryAfIdThread extends Thread {
	public static final Logger log = LoggerFactory.getLogger("WF");
	public static final Logger FLOW = LoggerFactory.getLogger("FLOW");
	
	public List<String> auList = null;
	public Map result = null;
	
	public QueryAfIdThread(List<String> auList) {
		this.auList = auList;
	}
	
	@Override
	public void run() {
		SenderImpl sender = new SenderImpl();
		result = sender.getAfIdList(auList);
	}
}
