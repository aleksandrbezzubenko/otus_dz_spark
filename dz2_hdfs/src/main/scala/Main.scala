
object Main extends App {

    val inputDataFolder = "/stage"
    val outputDataFolder = "/ods"

    FileSystem.createFolder(outputDataFolder)
    FileSystem.getFilesAndDirs(inputDataFolder)
      .filter(FileSystem.fileSystem.getFileStatus(_).isDirectory)
      .map(_.getName).foreach { dir =>
      println(s"Prepare dir: $dir")

      val currentDirPath = s"$inputDataFolder/$dir"
      val newDirPath = s"$outputDataFolder/$dir"
      FileSystem.createFolder(newDirPath)

      val files = FileSystem.getFilesAndDirs(currentDirPath)
        .filter(FileSystem.fileSystem.getFileStatus(_).isFile())
        .map(_.getName)
        .filter(!_.contains(".inprogress"))

      if (files.length > 0) {
        FileSystem.saveResult(files, currentDirPath, newDirPath)
      }
    }
}
