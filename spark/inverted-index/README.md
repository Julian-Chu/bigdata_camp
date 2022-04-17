# inverted index
output

![](img.png) \

```shell
/usr/local/Cellar/apache-spark/3.2.1/bin/spark-submit --master local --deploy-mode client --class SparkInvertedIndex --name spark-submit /Users/yu-langchu/Repos/bigdata_camp/spark/inverted-index/out/artifacts/inverted_index_jar/inverted-index.jar /Users/yu-langchu/Repos/bigdata_camp/spark/inverted-index/docs
```

## Description
```scala
    val files = sc.wholeTextFiles(input)
    /* RDD:(full file path,  file content)
    (file:/Users/yu-langchu/Repos/bigdata_camp/spark/inverted-index/docs/0,it is what it is)
    (file:/Users/yu-langchu/Repos/bigdata_camp/spark/inverted-index/docs/1,what is it)
    (file:/Users/yu-langchu/Repos/bigdata_camp/spark/inverted-index/docs/2,it is a banana)
    */
```
wholeTextFiles 讀取所有文件 生成 RDD(文件路徑, 文件內容)

```scala

val rdd1 = files.flatMapValues(_.split(" "))
  .map(x => ((x._1.split("/").last, x._2), 1))
  .reduceByKey((x, y) => x + y)
/* RDD: ((file, word), times)
((0,what),1)
((2,is),1)
((1,it),1)
...
 */
```
對文件內容以" "分割成單詞， 利用 flatMapValues 生成 RDD(文件路徑, 單詞)
對文件路徑以"/"切割，最後一個即為文件名
以文件名為 key 做 reduce
最後生成 RDD(文件名, 單詞, 詞頻) 
```scala
val rdd2 = rdd1.map(word => {
  (word._1._2, String.format("(%s,%s)", word._1._1, word._2.toString))
})
/* RDD: (word, (file, times))
  (what,(0,1))
  (is,(2,1))
  (it,(1,1))
 */
```
調整輸出格式 RDD(文件名, (單詞, 詞頻))
```scala
val rdd3 = rdd2.reduceByKey(_ + "," + _)
/* RDD: (word, (file, times), (file, times), (file, times) ....)
  (what,(0,1))
  (is,(2,1))
  (it,(1,1))
 */
```
依據文件名為 key 拼接 (單詞,詞頻)
```scala
val res = rdd3.map(rdd => String.format("\"%s\":{%s}", rdd._1, rdd._2))
res.foreach(println)
```
調整輸出格式