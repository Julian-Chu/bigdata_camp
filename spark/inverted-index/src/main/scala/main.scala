import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}

object SparkInvertedIndex {
  def main(args: Array[String]) = {
    //    args.foreach(println)
    var input: String = "/Users/yu-langchu/Repos/bigdata_camp/spark/inverted-index/docs"
    if (args.length > 1) {
      input = args.apply(0)
    }
    val sparkConf = new SparkConf().setAppName("invertedIndex").setMaster("local")

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val files = sc.wholeTextFiles(input)
    /* RDD:(full file path,  file content)
    (file:/Users/yu-langchu/Repos/bigdata_camp/spark/inverted-index/docs/0,it is what it is)
    (file:/Users/yu-langchu/Repos/bigdata_camp/spark/inverted-index/docs/1,what is it)
    (file:/Users/yu-langchu/Repos/bigdata_camp/spark/inverted-index/docs/2,it is a banana)
    */

    val rdd1 = files.flatMapValues(_.split(" "))
      .map(x => ((x._1.split("/").last, x._2), 1))
      .reduceByKey((x, y) => x + y)
    /* RDD: ((file, word), times)
    ((0,what),1)
    ((2,is),1)
    ((1,it),1)
    ...
     */

    val rdd2 = rdd1.map(word => {
      (word._1._2, String.format("(%s,%s)", word._1._1, word._2.toString))
    })
    /* RDD: (word, (file, times))
      (what,(0,1))
      (is,(2,1))
      (it,(1,1))
     */

    val rdd3 = rdd2.reduceByKey(_ + "," + _)
    /* RDD: (word, (file, times), (file, times), (file, times) ....)
      (what,(0,1))
      (is,(2,1))
      (it,(1,1))
     */

    val res = rdd3.map(rdd => String.format("\"%s\":{%s}", rdd._1, rdd._2))
    res.foreach(println)
  }
}
