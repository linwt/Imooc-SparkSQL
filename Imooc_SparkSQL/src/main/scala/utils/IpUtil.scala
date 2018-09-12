package utils

import com.ggstar.util.ip.IpHelper

object IpUtil {

  def getCity(ip: String) = {
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {
    print(getCity("58.30.10.225"))
  }

}
