package bigdata.hermesfuxi.datayi.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

object HDFSUtil {
  private val fileSystem: FileSystem = FileSystem.get(new Configuration())

  def main(args: Array[String]): Unit = {
    val localFilePath = this.getClass.getResource("/db/ip2region.db").getPath
    updateFile(localFilePath, "/datayi/dicts/")
  }

  def getFileSystem(): FileSystem = {
    fileSystem
  }

  def updateFile(localFilePath: String, hdfsFilePath: String) : Unit = {
    fileSystem.copyFromLocalFile(new Path(localFilePath), new Path(hdfsFilePath))
  }

  def listFileStatus(hdfsFilePath: String) : Unit = {
    val statuses: Array[FileStatus] = fileSystem.listStatus(new Path(hdfsFilePath))
  }

}
