package com.instaclustr.kafka.connect.s3.sink;

public class BeginOffsetTotalRecords {
    private long beginningOffset ;
    
    private long totalRecords;
    
	public BeginOffsetTotalRecords(long beginningOffset,long totalRecords) {
		this.beginningOffset = beginningOffset;
		this.totalRecords = totalRecords;
	} 
	
	public long getTotalRecords() {
		return totalRecords;
	}
	
	public void setTotalRecords(long totalRecords) {
		this.totalRecords = totalRecords;
	}
	
	public long getBeginningOffset() {
		return beginningOffset;
	}
	
	public void setBeginningOffset(long beginningOffset) {
		this.beginningOffset = beginningOffset;
	}

}
