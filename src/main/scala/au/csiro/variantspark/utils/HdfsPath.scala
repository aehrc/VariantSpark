package au.csiro.variantspark.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}

class HdfsPath(val fileName: String)(implicit hadoopConf: Configuration) {

  lazy val path: Path = new Path(fileName)
  lazy val fs: FileSystem = path.getFileSystem(hadoopConf)

  def open(): FSDataInputStream = fs.open(path)

  def create(overwrite: Boolean = true): FSDataOutputStream = fs.create(path, overwrite)
}

object HdfsPath {
  def apply(fileName: String)(implicit hadoopConf: Configuration): HdfsPath =
    new HdfsPath(fileName)(hadoopConf)
}
