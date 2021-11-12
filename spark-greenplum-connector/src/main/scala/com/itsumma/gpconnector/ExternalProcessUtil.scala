package com.itsumma.gpconnector

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.itsumma.gpconnector.GPOptionsFactory
import org.slf4j.LoggerFactory

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import scala.collection.JavaConversions._

case object ExternalProcessUtil
{
  def deleteRecursively(root: Path): Unit = {
    if (!Files.exists(root))
      return
    Files.walkFileTree(root, new SimpleFileVisitor[Path] {
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        Files.delete(file)
        FileVisitResult.CONTINUE
      }
      override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
        Files.delete(dir)
        FileVisitResult.CONTINUE
      }
    })
  }
}

case class ExternalProcessUtil(optionsFactory: GPOptionsFactory, workDir: Path, inheritStdIO: Boolean = false) {

  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  private def nextFreePort(): Int = {
    import java.net.ServerSocket
    var i = optionsFactory.serverPort
    var s: ServerSocket = null
    while ((i < 65535) && (s == null)) {
      try {
        s = new ServerSocket(i)
      } catch {
        case _: Exception => {
          s = null
          i += 1
        }
      }
    }
    if (s == null)
      return 0
    s.close()
    i
  }

  def gpfdistProcess(dumpEnv: Boolean = false): (Process, Int) = {
    var attempt: Int = 0
    val ret: Process = null
    while (attempt < 3) {
      try {
        val port: Int = nextFreePort()
        val pb = new ProcessBuilder().command(optionsFactory.serverPath, "-t", "5", "-w", "5", "-p", s"$port", "-d", workDir.toString, "-v")
        if (dumpEnv) {
          pb.environment().foreach(rec => {
            logger.info(s"ENV: ${rec._1}='${rec._2}")
          })
        }
        if (inheritStdIO)
          pb.inheritIO()
        return (pb.start(), port)
      } catch {
        case e: Exception =>
      }
      attempt += 1
    }
    (ret, 0)
  }
}
