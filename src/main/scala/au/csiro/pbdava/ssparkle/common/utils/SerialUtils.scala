package au.csiro.pbdava.ssparkle.common.utils

import java.io.{InputStream, ObjectInputStream, ObjectOutputStream, OutputStream}

import scala.reflect.ClassTag

object SerialUtils {
  def write[T](s: T, outStream: => OutputStream) {
    LoanUtils.withCloseable(new ObjectOutputStream(outStream)) { out => out.writeObject(s) }
  }

  def read[T](inStream: => InputStream)(implicit t: ClassTag[T]): T = {
    LoanUtils.withCloseable(new ObjectInputStream(inStream)) { in =>
      in.readObject().asInstanceOf[T]
    }
  }
}
