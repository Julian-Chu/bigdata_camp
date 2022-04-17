# ScalaDistCp (未完成)

思路： 借鑑助教的範例，增加 parseOpts

```shell
/usr/local/Cellar/apache-spark/3.2.1/bin/spark-submit --master local --deploy-mode client --class DistCp --name Unnamed /Users/yu-langchu/Repos/bigdata_camp/spark/ScalaDistCP/out/artifacts/ScalaDistCP_jar/ScalaDistCP.jar --source file://Users/yu-langchu/Repos/bigdata_camp/spark/ScalaDistCp/docs --target file://Users/yu-langchu/Repos/bigdata_camp/spark/ScalaDistCp/docs2
```

spark-submit 錯誤無法正確運作，當前環境為 spark 3.2, 使用 sbt
```shell
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/usr/local/Cellar/apache-spark/3.2.1/libexec/jars/spark-unsafe_2.12-3.2.1.jar) to constructor java.nio.DirectByteBuffer(long,int)
WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
Error: Failed to load class DistCp.
log4j:WARN No appenders could be found for logger (org.apache.spark.util.ShutdownHookManager).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
```