package au.csiro.pbdava.ssparkle.common.utils

import java.io.PrintStream
import java.io.OutputStream

class ReusablePrintStream(stream: OutputStream) extends PrintStream(stream) {

  override def close() {
    // just flush do not close
    stream.flush()
  }
}

object ReusablePrintStream {
  lazy val stdout = new ReusablePrintStream(System.out)
}
