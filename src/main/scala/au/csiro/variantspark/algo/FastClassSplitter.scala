package au.csiro.variantspark.algo

import java.util.Arrays

class FastClassSplitter {
    val leftSplitCounts = Array.fill(2)(0);
		val rightSplitCounts = Array.fill(2)(0);

		
	 def ginit(counts:Array[Int]):Double = {
		 if (counts.length == 2) {
		      val c1 = counts(0);
    	      val c2 = counts(1);
    	      val total = (c1+c2);
    	      val p1 = (c1.toDouble)/total;
    	      val p2 = (c2.toDouble)/total;
    	      return if (total == 0) 0.0 else  (1.0 - p1*p1 - p2*p2);			 
		 } else {
			 throw new RuntimeException("Not imlemented");
		 }
	 }
	 
	 def gini(left:Array[Int], right:Array[Int]):Double =  {
		 val lt = left(0) + left(1);
		 val rt = right(0) + right(1);	
		 return (ginit(left)*lt + ginit(right)*lt)/(lt+rt);
	 }
	 
	 def  findSplit(data:Array[Int],splitIndices:Array[Int], nCategories:Int, labels:Array[Int]):SplitInfo =  {
			    // essentialy we need to find the best split for data and labels where splits[i] = splitIndex
			    // would be nice perhaps if I couild easily subset my vectors    
			    // TODO: make an explicit parameter (also can subset by current split) --> essentially could be unique variable values
			    
			    // TODO (Optimize): it make sense to use a range instead of set (especially for 0,1,2) datasets
			    // Even though it man mean running more additions
	
	  var result:SplitInfo = null;
	  var  minGini:Double = 1.0;
		for(sp <- 0 until 2) {
			Arrays.fill(leftSplitCounts, 0);
			Arrays.fill(rightSplitCounts, 0);
			for(i <- splitIndices) {
				if (data(i) <=sp) {
					leftSplitCounts(labels(i))+=1;
				} else {
					rightSplitCounts(labels(i))+=1;					
				}
			}
			val g = gini(leftSplitCounts, rightSplitCounts);
			if (g < minGini ) {
				result = new SplitInfo(sp, g, 0.0, 0.0);
			}
		}
		return result;
	 }
}