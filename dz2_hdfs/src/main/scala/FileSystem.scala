import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem => FS, FileUtil, Path}

import java.io.BufferedInputStream
import java.net.URI

object FileSystem {

  System.setProperty("hadoop.home.dir", "/")

  val mainHDFSHost = "hdfs://localhost:9000"

  val conf = new Configuration()

  def fileSystem: FS = {
    FS.get(new URI(mainHDFSHost), conf)
  }

  def createFolder(folderName: String): Unit = {
    val path = new Path(folderName)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
      println(s"Create folder $folderName")
    } else {
      println(s"Folder $folderName already exists")
    }
  }

  def createFile(fileName: String): Unit = {
    val path = new Path(fileName)
    if (!fileSystem.exists(path)) {
      fileSystem.createNewFile(path)
      println(s"Create file $fileName")
    } else {
      println(s"File $fileName already exists")
    }
  }

  def getFilesAndDirs(path: String): Array[Path] = {
    val fs = fileSystem.listStatus(new Path(path))
    FileUtil.stat2Paths(fs)
  }

  def saveResult(files: Array[String], fromDirPath: String, toDirPath: String): Unit = {
    val fileName = files.head
    val newFilePath = s"$toDirPath/$fileName"

    createFile(newFilePath)

    val outFile = fileSystem.append(new Path(newFilePath))
    for (file <- files) {
      val currentFilePath = s"$fromDirPath/$file"
      val inFile = new BufferedInputStream(fileSystem.open(new Path(currentFilePath)))
      val b = new Array[Byte](1024)
      var numBytes = inFile.read(b)
      while (numBytes > 0) {
        outFile.write(b, 0, numBytes)
        numBytes = inFile.read(b)
      }
      inFile.close()
    }
    outFile.close()
  }

}
