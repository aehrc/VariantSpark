package au.csiro.pbdava.ssparkle.common.utils

import java.io.Closeable

object LoanUtils {

  def withCloseable[C <: Closeable,R](cl:C)(func:C => R):R = {
    try {
      func(cl)
    } finally {
      cl.close()
    }
  }  
}