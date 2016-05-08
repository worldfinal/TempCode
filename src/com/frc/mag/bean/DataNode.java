package com.frc.mag.bean;

public class DataNode {
	public long val;
	public short type;
	public long from;

	public DataNode(long val, short type) {
		this.val = val;
		this.type = type;
	}

	public DataNode(long val, short type, long from) {
		this.val = val;
		this.type = type;
		this.from = from;
	}

	public DataNode() {

	}
}
