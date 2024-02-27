package com.itsumma.gpconnector.rmi

import org.apache.spark.internal.Logging

import java.net.{DatagramSocket, InetAddress, InterfaceAddress, NetworkInterface, SocketException}

case class NetUtils() extends Logging {

  /**
   * Waits until condition become true or msMax milliseconds elapsed
   * @param msMax
   * @param fun lambda function returning Boolean
   * @return false if timeout elapsed or true otherwise
   */
  def waitForCompletion(msMax: Long = 60000)(fun: => Boolean): Boolean = {
    val start = System.currentTimeMillis()
    val rnd = new scala.util.Random
    var result: Boolean = false
    while (!result &&
      !Thread.currentThread().isInterrupted &&
      (((System.currentTimeMillis() - start) < msMax) || (msMax == -1))) {
      result = fun
      if (!result)
        Thread.sleep(rnd.nextInt(10) + 1)
    }
    result
  }

  /**
   * Get tuple of hostName, ipAddress associated with the local host,
   * choosing the default route network interface if possible.
   *
   * @return (hostName, ipAddress)
   */
  def getLocalHostNameAndIp: (String, String) = {
    var retName: String = null
    var retIp: String = null
    try {
      val networkInterfaceEnumeration = NetworkInterface.getNetworkInterfaces
      while (networkInterfaceEnumeration.hasMoreElements) {
        import scala.collection.JavaConverters._
        val netInterface: NetworkInterface = networkInterfaceEnumeration.nextElement
        if (netInterface.isUp && !netInterface.isLoopback) {
          for (interfaceAddress: InterfaceAddress <- netInterface.getInterfaceAddresses.asScala) {
            val inetAddress: InetAddress = interfaceAddress.getAddress
            if (inetAddress.isSiteLocalAddress && !inetAddress.isLoopbackAddress) {
              val hostName = inetAddress.getHostName
              val ipAddress = inetAddress.getHostAddress
              if (hostName != ipAddress) {
                if (retName == null) {
                  retName = hostName
                  retIp = ipAddress
                }
                try {
                  val s: DatagramSocket = new DatagramSocket()
                  s.connect(InetAddress.getByAddress(Array[Byte](1, 1, 1, 1)), 0)
                  if (s.getLocalAddress.getHostAddress == ipAddress)
                    return (hostName, ipAddress)
                } catch {
                  case e: Exception => logDebug(s"${e.getMessage}")
                }
              }
            }
          }
        }
      }
    } catch {
      case e: SocketException =>
        logDebug(s"${e.getMessage}")
    }
    if (retIp == null || retName == null) {
      retIp = InetAddress.getLocalHost.getHostAddress
      retName = InetAddress.getLocalHost.getHostName
    }
    (retName, retIp)
  }

  /**
   * Resolves given host name to IP address.
   * In the case of local host name tries to avoid loopback address and choose
   * IP associated with some other interface of same host if possible.
   *
   * @param hostName
   * @return IP as string
   */
  def resolveHost2Ip(hostName: String): String = {
    var isLocal: Boolean = false
    for (address <- InetAddress.getAllByName(hostName)) {
      if (!address.isLoopbackAddress) {
        val ip = address.getHostAddress
        logInfo(s"Resolved ${hostName} to ${ip}")
        return ip
      }
      if (address.isLoopbackAddress || address.isSiteLocalAddress) {
        isLocal = true
      }
    }
    if (isLocal) {
      val (name, ip) = getLocalHostNameAndIp
      logInfo(s"Resolved ${hostName} to local ${ip}/${name}")
      return ip
    }
    logInfo(s"Unable to resolve ${hostName}")
    null
  }
}
