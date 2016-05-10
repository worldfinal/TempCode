package com.frc.mag.send;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.frc.mag.bean.DataNode;
import com.frc.mag.parse.IConstants;
import com.frc.mag.thread.QueryThread;

public class SenderImpl {
	//get list of Id from list of RId
	public Map getIdList(List<String> ridArr) {
		int i, j, k = 0, n = ridArr.size();
		int MAXN = 40;	//一次处理MAXN个RId
		List<QueryThread> threadList = new ArrayList<QueryThread>();
		for (i = 0, k = 0; k < n && i < n / MAXN; i++) {
			String cond = "";
			cond = "RId=" + ridArr.get(k);
			k++;
			for (j = 1; j < MAXN && k < n; j++) {
				String str = String.format("Or(%s,RId=%s)", cond, ridArr.get(k));
				k++;
				cond = str;
			}
			QueryThread thread = new QueryThread(cond, IConstants.COMMON_ATTR, IConstants.MAX_COUNT, "0");
			thread.start();
			threadList.add(thread);
		}
		
		try {
			for (i = 0; i < threadList.size(); i++) {
				QueryThread thread = threadList.get(i);
				thread.join();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		Map result = new HashMap();
		for (i = 0; i < threadList.size(); i++) {
			QueryThread thread = threadList.get(i);
			Map rs = thread.getResult();
			if (rs != null) {
				result.putAll(rs);
			}
		}
		
		return result;
	}
	
	public Map getAfIdList(List<String> auList) {
		int i, j, k = 0, n = auList.size();
		int MAXN = 40;	//一次处理MAXN个AuId
		List<QueryThread> threadList = new ArrayList<QueryThread>();
		for (i = 0, k = 0; k < n && i < n / MAXN; i++) {
			String cond = "";
			cond = String.format("Composite(AA.AuId=%s)", auList.get(k));
			k++;
			for (j = 1; j < MAXN && k < n; j++) {
				String str = String.format("Or(%s,Composite(AA.AuId=%s))", cond, auList.get(k));
				k++;
				cond = str;
			}
			QueryThread thread = new QueryThread(cond, IConstants.COMMON_ATTR, IConstants.MAX_COUNT, "0");
			thread.start();
			threadList.add(thread);
		}
		
		try {
			for (i = 0; i < threadList.size(); i++) {
				QueryThread thread = threadList.get(i);
				thread.join();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		Map result = new HashMap();
		for (i = 0; i < threadList.size(); i++) {
			QueryThread thread = threadList.get(i);
			Map rs = thread.getResult();
			if (rs != null) {
				result.putAll(rs);
			}
		}
		
		return result;
	}
	
	protected String generateExpr(DataNode node) {
		String rs = "";
		switch (node.type) {
		case IConstants.SHORT_AFID:
			rs = String.format("Composite(AA.AfId=%d)", node.val);
			break;
		case IConstants.SHORT_AUID:
			rs = String.format("Composite(AA.AuId=%d)", node.val);
			break;
		case IConstants.SHORT_CID:
			rs = String.format("CId=%d", node.val);
			break;
		case IConstants.SHORT_FID:
			rs = String.format("Composite(F.FId=%d)", node.val);
			break;
		case IConstants.SHORT_JID:
			rs = String.format("CId=%d", node.val);
			break;
		}
		return rs;
	}
}
