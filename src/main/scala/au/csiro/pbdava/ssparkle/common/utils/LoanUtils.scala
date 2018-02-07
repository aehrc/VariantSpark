package au.csiro.pbdava.ssparkle.common.utils

import java.io.Closeable
import scala.io.Source

object LoanUtils {

  def withSource[R](src: Source)(func: Source => R):R = {
    try {
      func(src)
    } finally {
      src.close()
    }    
  }
  
  def withCloseable[C <: Closeable,R](cl:C)(func:C => R):R = {
    try {
      func(cl)
    } finally {
      cl.close()
    }
  }  
}