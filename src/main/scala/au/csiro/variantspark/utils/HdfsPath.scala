package au.csiro.variantspark.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.Path


class HdfsPath(val fileName:String)(implicit  hadoopConf:Configuration) {
 
  lazy val path =  new Path(fileName)
  lazy val fs = path.getFileSystem(hadoopConf)
  
    def open():FSDataInputStream = fs.open(path)
   
    def create(overwrite:Boolean = true)= fs.create(path, overwrite)
}

object HdfsPath {
  def apply(fileName:String)(implicit  hadoopConf:Configuration) = new HdfsPath(fileName)(hadoopConf)
}

