package au.csiro.variantspark.cmd

object EchoUtils {

  val defaultPreviewSize = 7  
  val longPreviewSize = 20
  val defaultElipses = "..."
  
  def dumpList(l:List[_], maxSize:Int = defaultPreviewSize):String = {

      val strList = if (l.size <= maxSize) l.map(_.toString) else  l.take(maxSize/2).map(_.toString) ::: (defaultElipses :: l.takeRight(maxSize -  maxSize/2 -1).map(_.toString))
      strList.mkString("[", ",", "]") + s" total: ${l.size}"
  }

  def dumpListHead(l:List[_], totalSize:Long, maxSize:Int = defaultPreviewSize):String = {

      val strList = if (totalSize <= maxSize) l.map(_.toString) else l.take(maxSize - 1).map(_.toString) ::: List(defaultElipses) 
      strList.mkString("[", ",", "]") + s" total: ${totalSize}"
  }
  
  
}