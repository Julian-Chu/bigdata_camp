import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer

object DistCp {
  def main(args: Array[String]): Unit = {
    val opts = parseOpts(args)
    val conf = new SparkConf().setMaster("local").setAppName("DistCp")
    val sc = new SparkContext(conf)
    val fileList = new ArrayBuffer[(Path, Path)]()
    val sourcePath = new Path(opts.source)
    val targetPath = new Path(opts.target)

    makeDir(sc, sourcePath, targetPath, fileList, opts)
    copy(sc, fileList, opts)
  }

  def makeDir(sc: SparkContext, sourcePath: Path, targetPath: Path, fileList: ArrayBuffer[(Path, Path)], options: SparkDistCpOptions): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.listStatus(sourcePath)
      .foreach(currPath => {
        if (currPath.isDirectory) {
          val subPath = currPath.getPath.toString.split(sourcePath.toString)(1)
          val nextTargetPath = new Path(targetPath + subPath)
          try {
            fs.mkdirs(nextTargetPath)
          } catch {
            case ex: Exception => if (!options.ignoreFailures) throw ex else println("Exception: %s", ex.getMessage)
          }
          makeDir(sc, currPath.getPath, nextTargetPath, fileList, options)
        } else {
          fileList.append((currPath.getPath, targetPath))
        }
      })
  }

  def copy(sc: SparkContext, fileList: ArrayBuffer[(Path, Path)], options: SparkDistCpOptions): Unit = {
    val maxConcurrenceTask = Some(options.maxConcurrenceTask).getOrElse(5)
    val rdd = sc.makeRDD(fileList, maxConcurrenceTask)

    rdd.mapPartitions(ite => {
      val hadoopConf = new Configuration()
      ite.foreach(tup => {
        try {
          FileUtil.copy(tup._1.getFileSystem(hadoopConf), tup._1, tup._2.getFileSystem(hadoopConf),
            tup._2, false, hadoopConf)
        } catch {
          case ex: Exception => if (!options.ignoreFailures) throw ex else println("Exception: %s", ex.getMessage)
        }
      })
      ite
    }).collect()
  }

  def parseOpts(args: Array[String]): SparkDistCpOptions = {
    var source = ""
    var target = ""
    var maxConcurrenceTask = 4
    var ignoreFailures = true

    args.sliding(2, 2).toList.collect {
      case Array("--source", value: String) => source = value
      case Array("--target", value: String) => target = value
      case Array("-i", value: String) => ignoreFailures = value.toBoolean
      case Array("-m", value: String) => maxConcurrenceTask = value.toInt
    }
    if (source == null || source.trim.isEmpty) {
      println("Error: source should not be empty")
      sys.exit(1)
    }
    if (target == null || target.trim.isEmpty) {
      println("Error: source should not be empty")
      sys.exit(1)
    }
    SparkDistCpOptions(source, target, maxConcurrenceTask, ignoreFailures)
  }
}


case class SparkDistCpOptions(source: String, target: String, maxConcurrenceTask: Int, ignoreFailures: Boolean) {}