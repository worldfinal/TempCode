package com.frc.mag.bean;

import java.util.List;

public class BigDataNode {
      public long val;
      public List<Long> ridList;
      public BigDataNode (long val, List<Long> ridList) {
             this.val = val;
             this.ridList = ridList;
      }
      public BigDataNode() {
             
      }
}
