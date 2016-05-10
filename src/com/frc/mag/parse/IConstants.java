package com.frc.mag.parse;

public interface IConstants {
	public static final String TESTPATH = "D:\\搜狗高速下载\\MAG";
//	public static final String TESTPATH = "/home/dannyfeng/MAGfile";
	public static final short SHORT_ID = 0;
	public static final short SHORT_AUID = 1;
	public static final short SHORT_FID = 2;
	public static final short SHORT_CID = 3;
	public static final short SHORT_JID = 4;
	public static final short SHORT_AFID = 5;

	public static final String MAX_COUNT = "500"; // 最大记录数
	public static final int RETRY_TIME = 3; // 重试次数
	
	public static final String COMMON_ATTR = "Ti,Y,CC,Id,AA.AuId,AA.AfId,J.JId,C.CId,F.FId,RId";

	public static final boolean TEST = false;
	public static final boolean LOG = true;
}
