package utils

object StringUtils {
  /**
   * Convert CLI arguments into a Map[String, String]
   * @param argument The String argument in the format "key=value".
   * @return An Option containing a tuple (key, value) if the argument is in the correct format, None otherwise.
   */
  def convertArgsToMap(argument: String): Option[(String, String)] = {
    val parts = argument.split("=")
    if (parts.length == 2)
      Some(parts(0) -> parts(1))
    else
      None
  }
}
