package au.csiro.variantspark.utils

/** Utilities for efficient byte-level parsing of text-based biological file formats.
  * These methods avoid String allocation for memory-efficient parsing.
  */
object ByteParsingUtils {

  val TAB: Byte = '\t'.toByte
  val HASH: Byte = '#'.toByte
  val COLON: Byte = ':'.toByte
  val DOT: Byte = '.'.toByte
  val ZERO: Byte = '0'.toByte
  val NEWLINE: Byte = '\n'.toByte
  val CARRIAGE_RETURN: Byte = '\r'.toByte

  /** Find byte in array, returns len if not found */
  @inline def findByte(arr: Array[Byte], b: Byte, from: Int, len: Int): Int = {
    var i = from
    while (i < len && arr(i) != b) i += 1
    i
  }

  /** Parse an integer directly from bytes without String allocation */
  @inline def parseIntFromBytes(arr: Array[Byte], start: Int, end: Int): Int = {
    var result = 0
    var i = start
    while (i < end) {
      result = result * 10 + (arr(i) - '0')
      i += 1
    }
    result
  }

  /** Count occurrences of a byte in a range */
  @inline def countByte(arr: Array[Byte], b: Byte, from: Int, len: Int): Int = {
    var count = 0
    var i = from
    while (i < len) {
      if (arr(i) == b) count += 1
      i += 1
    }
    count
  }

  /** Extract substring from byte array as interned String for memory efficiency */
  @inline def internedString(arr: Array[Byte], start: Int, end: Int,
      charset: String = "UTF-8"): String = {
    new String(arr, start, end - start, charset).intern()
  }

  /** Extract substring from byte array */
  @inline def byteString(arr: Array[Byte], start: Int, end: Int,
      charset: String = "UTF-8"): String = {
    new String(arr, start, end - start, charset)
  }
}
