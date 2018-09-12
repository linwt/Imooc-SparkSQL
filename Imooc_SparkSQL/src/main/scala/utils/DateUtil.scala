package utils

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

object DateUtil {

  val source = FastDateFormat.getInstance("dd/MM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  val target = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def parse(time: String): String = {
    target.format(new Date(getTime(time)))
  }

  def getTime(time: String): Long = {
    source.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime
  }

  def main(args: Array[String]): Unit = {
    print(parse("[11/05/2017:00:01:02 +0800]"))
  }
}
