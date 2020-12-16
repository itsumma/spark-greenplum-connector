package com.itsumma.gpconnector

import java.nio.file.Path

case class ExternalProcessUtil(workDir: Path, inheritStdIO: Boolean = false) {
  def gpfdistProcess(port: Int): ProcessBuilder = {
    val pb = new ProcessBuilder().command("gpfdist","-p",s"$port","-d",workDir.toString)
    if (!inheritStdIO)
      return pb
    pb.inheritIO()
  }
}
