# Spark SQL Homework
## Homework1: show version

[github commit](https://github.com/Julian-Chu/spark/commit/73de375f1b55df4b13112d7b844626d411190e6f)
1. 修改 sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4 
2. 執行 maven -> Spark Project Catalyst -> Plugins -> antlr4
3. 新增 scala code
4. 編譯命令： `build/mvn clean package -DskipTests -Phive -Phive-thriftservr`

![show version](./show-version.png)

## Homework2: optimizer rules
### 1. CombineFilters, CollapseProject、BooleanSimplification
```json
  { "ID": 1, "Name": "Max", "Amount": 1.23, "Age": 20 },
  { "ID": 2, "Name": "John", "Amount": 200.00, "Age": 15 },
  { "ID": 3, "Name": "Mary", "Amount": 106.00, "Age": 30 },
  { "ID": 4, "Name": "Ada", "Amount": 22.00, "Age": 18 }
```

in spark-sql
```sql
set spark.sql.planChangeLog.level=WARN;
CREATE TEMPORARY VIEW record USING org.apache.spark.sql.json  OPTIONS (path 'data.json');
SELECT  NewAge FROM (SELECT Age+1 as NewAge FROM (select Age FROM record WHERE Age > 15 )) WHERE NewAge > 18 AND 1=1;
```

```shell
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.PushDownPredicates ===
!Filter ((NewAge#97L > cast(18 as bigint)) AND (1 = 1))         Project [(Age#34L + cast(1 as bigint)) AS NewAge#97L]
!+- Project [(Age#34L + cast(1 as bigint)) AS NewAge#97L]       +- Project [Age#34L]
!   +- Project [Age#34L]                                           +- Filter ((Age#34L > cast(15 as bigint)) AND (((Age#34L + cast(1 as bigint)) > cast(18 as bigint)) AND (1 = 1)))
!      +- Filter (Age#34L > cast(15 as bigint))                       +- Relation [Age#34L,Amount#35,ID#36L,Name#37] json
!         +- Relation [Age#34L,Amount#35,ID#36L,Name#37] json

22/05/08 00:59:37 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.CollapseProject ===
 Project [(Age#34L + cast(1 as bigint)) AS NewAge#97L]                                                                  Project [(Age#34L + cast(1 as bigint)) AS NewAge#97L]
!+- Project [Age#34L]                                                                                                   +- Filter ((Age#34L > cast(15 as bigint)) AND (((Age#34L + cast(1 as bigint)) > cast(18 as bigint)) AND (1 = 1)))
!   +- Filter ((Age#34L > cast(15 as bigint)) AND (((Age#34L + cast(1 as bigint)) > cast(18 as bigint)) AND (1 = 1)))      +- Relation [Age#34L,Amount#35,ID#36L,Name#37] json
!      +- Relation [Age#34L,Amount#35,ID#36L,Name#37] json

..... 

22/05/08 00:59:37 WARN PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.BooleanSimplification ===
 Project [(Age#34L + 1) AS NewAge#97L]                            Project [(Age#34L + 1) AS NewAge#97L]
!+- Filter ((Age#34L > 15) AND (((Age#34L + 1) > 18) AND true))   +- Filter ((Age#34L > 15) AND ((Age#34L + 1) > 18))
    +- Relation [Age#34L,Amount#35,ID#36L,Name#37] json              +- Relation [Age#34L,Amount#35,ID#36L,Name#37] json
```

### 2. ConstantFolding、PushDownPredicates、ReplaceDistinctWithAggregate、 ReplaceExceptWithAntiJoin、FoldablePropagation
使用助教的範例，研究 log， 一步步展開優化規則對 sql 的改寫

```sql
CREATE TABLE t1(a1 INT, a2 INT) USING parquet;
CREATE TABLE t2(b1 INT, b2 INT) USING parquet;
SELECT DISTINCT a1, a2, 'custom' a3 FROM ( SELECT * FROM t1 WHERE a2 = 10 AND 1 = 1 ) WHERE a1 > 5 AND 1 = 1 EXCEPT SELECT b1, b2, 1.0 b3 FROM t2 WHERE b2 = 10 ;
```

```shell
22/05/08 11:15:15 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ReplaceExceptWithAntiJoin ===
!Except false                                                Distinct
!:- Distinct                                                 +- Join LeftAnti, (((a1#0 <=> b1#18) AND (a2#1 <=> b2#19)) AND (a3#16 <=> b3#20))
!:  +- Project [a1#0, a2#1, custom AS a3#16]                    :- Distinct
!:     +- Filter ((a1#0 > 5) AND (1 = 1))                       :  +- Project [a1#0, a2#1, custom AS a3#16]
!:        +- Filter ((a2#1 = 10) AND (1 = 1))                   :     +- Filter ((a1#0 > 5) AND (1 = 1))
!:           +- Relation default.t1[a1#0,a2#1] parquet          :        +- Filter ((a2#1 = 10) AND (1 = 1))
!+- Project [b1#18, b2#19, cast(b3#17 as string) AS b3#20]      :           +- Relation default.t1[a1#0,a2#1] parquet
!   +- Project [b1#18, b2#19, 1.0 AS b3#17]                     +- Project [b1#18, b2#19, cast(b3#17 as string) AS b3#20]
!      +- Filter (b2#19 = 10)                                      +- Project [b1#18, b2#19, 1.0 AS b3#17]
!         +- Relation default.t2[b1#18,b2#19] parquet                 +- Filter (b2#19 = 10)
!                                                                        +- Relation default.t2[b1#18,b2#19] parquet
```

`SELECT DISTINCT a1, a2, 'custom' a3 FROM ( SELECT * FROM t1 WHERE a2 = 10 AND 1 = 1 ) WHERE a1 > 5 AND 1 = 1 
LEFT ANTI JOIN (SELECT b1, b2, 1.0 b3 FROM t2 WHERE b2 = 10) on  a1<=>b1, a2<=>b2, a3<=>b3(1.0)`

```shell
22/05/08 11:15:15 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ReplaceDistinctWithAggregate ===
!Distinct                                                                            Aggregate [a1#0, a2#1, a3#16], [a1#0, a2#1, a3#16]
 +- Join LeftAnti, (((a1#0 <=> b1#18) AND (a2#1 <=> b2#19)) AND (a3#16 <=> b3#20))   +- Join LeftAnti, (((a1#0 <=> b1#18) AND (a2#1 <=> b2#19)) AND (a3#16 <=> b3#20))
!   :- Distinct                                                                         :- Aggregate [a1#0, a2#1, a3#16], [a1#0, a2#1, a3#16]
    :  +- Project [a1#0, a2#1, custom AS a3#16]                                         :  +- Project [a1#0, a2#1, custom AS a3#16]
    :     +- Filter ((a1#0 > 5) AND (1 = 1))                                            :     +- Filter ((a1#0 > 5) AND (1 = 1))
    :        +- Filter ((a2#1 = 10) AND (1 = 1))                                        :        +- Filter ((a2#1 = 10) AND (1 = 1))
    :           +- Relation default.t1[a1#0,a2#1] parquet                               :           +- Relation default.t1[a1#0,a2#1] parquet
    +- Project [b1#18, b2#19, cast(b3#17 as string) AS b3#20]                           +- Project [b1#18, b2#19, cast(b3#17 as string) AS b3#20]
       +- Project [b1#18, b2#19, 1.0 AS b3#17]                                             +- Project [b1#18, b2#19, 1.0 AS b3#17]
          +- Filter (b2#19 = 10)                                                              +- Filter (b2#19 = 10)
             +- Relation default.t2[b1#18,b2#19] parquet                                         +- Relation default.t2[b1#18,b2#19] parquet
```
`SELECT a1, a2, 'custom' a3 FROM ( SELECT * FROM t1 WHERE a2 = 10 AND 1 = 1 ) WHERE a1 > 5 AND 1 = 1  LEFT ANTI JOIN (SELECT b1, b2, 1.0 b3 FROM t2 WHERE b2 = 10) on  a1<=>b1, a2<=>b2, a3<=>b3(1.0) GROUP BY a1, a2, a3`

```shell
22/05/08 11:15:15 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.PushDownPredicates ===
 Aggregate [a1#0, a2#1, a3#16], [a1#0, a2#1, a3#16]                                  Aggregate [a1#0, a2#1, a3#16], [a1#0, a2#1, a3#16]
 +- Join LeftAnti, (((a1#0 <=> b1#18) AND (a2#1 <=> b2#19)) AND (a3#16 <=> b3#20))   +- Join LeftAnti, (((a1#0 <=> b1#18) AND (a2#1 <=> b2#19)) AND (a3#16 <=> b3#20))
    :- Aggregate [a1#0, a2#1, a3#16], [a1#0, a2#1, a3#16]                               :- Aggregate [a1#0, a2#1, a3#16], [a1#0, a2#1, a3#16]
    :  +- Project [a1#0, a2#1, custom AS a3#16]                                         :  +- Project [a1#0, a2#1, custom AS a3#16]
!   :     +- Filter ((a1#0 > 5) AND (1 = 1))                                            :     +- Filter (((a2#1 = 10) AND (1 = 1)) AND (a1#0 > 5))
!   :        +- Filter ((a2#1 = 10) AND (1 = 1))                                        :        +- Relation default.t1[a1#0,a2#1] parquet
!   :           +- Relation default.t1[a1#0,a2#1] parquet                               +- Project [b1#18, b2#19, cast(b3#17 as string) AS b3#20]
!   +- Project [b1#18, b2#19, cast(b3#17 as string) AS b3#20]                              +- Project [b1#18, b2#19, 1.0 AS b3#17]
!      +- Project [b1#18, b2#19, 1.0 AS b3#17]                                                +- Filter (b2#19 = 10)
!         +- Filter (b2#19 = 10)                                                                 +- Relation default.t2[b1#18,b2#19] parquet
!            +- Relation default.t2[b1#18,b2#19] parquet   
```

`SELECT a1, a2, 'custom' a3 FROM ( SELECT * FROM t1 WHERE a2 = 10 AND 1 = 1 AND a1 > 5 ) LEFT ANTI JOIN (SELECT b1, b2, 1.0 b3 FROM t2 WHERE b2 = 10) on a1<=>b1, a2<=>b2, a3<=>b3(1.0) GROUP BY a1, a2, a3`
```shell
22/05/08 11:15:16 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.FoldablePropagation ===
!Aggregate [a1#0, a2#1, a3#16], [a1#0, a2#1, a3#16]                                         Aggregate [a1#0, a2#1, custom], [a1#0, a2#1, custom AS a3#16]
!+- Aggregate [a1#0, a2#1, a3#16], [a1#0, a2#1, a3#16]                                      +- Aggregate [a1#0, a2#1, custom], [a1#0, a2#1, custom AS a3#16]
    +- Project [a1#0, a2#1, custom AS a3#16]                                                   +- Project [a1#0, a2#1, custom AS a3#16]
!      +- Join LeftAnti, (((a1#0 <=> b1#18) AND (a2#1 <=> b2#19)) AND (custom <=> b3#20))         +- Join LeftAnti, (((a1#0 <=> b1#18) AND (a2#1 <=> b2#19)) AND (custom <=> cast(1.0 as string)))
          :- Filter (((a2#1 = 10) AND (1 = 1)) AND (a1#0 > 5))                                       :- Filter (((a2#1 = 10) AND (1 = 1)) AND (a1#0 > 5))
          :  +- Relation default.t1[a1#0,a2#1] parquet                                               :  +- Relation default.t1[a1#0,a2#1] parquet
          +- Project [b1#18, b2#19, cast(1.0 as string) AS b3#20]                                    +- Project [b1#18, b2#19, cast(1.0 as string) AS b3#20]
             +- Filter (b2#19 = 10)                                                                     +- Filter (b2#19 = 10)
                +- Relation default.t2[b1#18,b2#19] parquet                                                +- Relation default.t2[b1#18,b2#19] parquet
```
`SELECT a1, a2, 'custom' FROM ( SELECT * FROM t1 WHERE a2 = 10 AND 1 = 1 AND a1 > 5 ) LEFT ANTI JOIN (SELECT b1, b2, 1.0 b3 FROM t2 WHERE b2 = 10) on a1<=>b1, a2<=>b2, custom<=>cast(1.0 as string) GROUP BY a1, a2, 'custom`

```shell
22/05/08 11:15:16 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ConstantFolding ===
 Aggregate [a1#0, a2#1, custom], [a1#0, a2#1, custom AS a3#16]                                            Aggregate [a1#0, a2#1, custom], [a1#0, a2#1, custom AS a3#16]
 +- Aggregate [a1#0, a2#1, custom], [a1#0, a2#1, custom AS a3#16]                                         +- Aggregate [a1#0, a2#1, custom], [a1#0, a2#1, custom AS a3#16]
    +- Project [a1#0, a2#1, custom AS a3#16]                                                                 +- Project [a1#0, a2#1, custom AS a3#16]
!      +- Join LeftAnti, (((a1#0 <=> b1#18) AND (a2#1 <=> b2#19)) AND (custom <=> cast(1.0 as string)))         +- Join LeftAnti, (((a1#0 <=> b1#18) AND (a2#1 <=> b2#19)) AND false)
!         :- Filter (((a2#1 = 10) AND (1 = 1)) AND (a1#0 > 5))                                                     :- Filter (((a2#1 = 10) AND true) AND (a1#0 > 5))
          :  +- Relation default.t1[a1#0,a2#1] parquet                                                             :  +- Relation default.t1[a1#0,a2#1] parquet
!         +- Project [b1#18, b2#19, cast(1.0 as string) AS b3#20]                                                  +- Project [b1#18, b2#19, 1.0 AS b3#20]
             +- Filter (b2#19 = 10)                                                                                   +- Filter (b2#19 = 10)
                +- Relation default.t2[b1#18,b2#19] parquet    
```

`SELECT a1, a2, 'custom' FROM ( SELECT * FROM t1 WHERE a2 = 10 AND true AND a1 > 5 ) LEFT ANTI JOIN (SELECT b1, b2, 1.0 b3 FROM t2 WHERE b2 = 10) on a1<=>b1, a2<=>b2, false GROUP BY a1, a2, 'custom`
## Homework3: customized optimization
[source code](../spark-sql-extension)

`spark-sql --jars ./out/artifacts/spark_sql_extension_jar/spark-sql-extension.jar --conf spark.sql.extensions=MySparkSessionExtension`

出現 ClassNotFoundException,  查問題中 
```shell
22/05/08 11:56:43 WARN SparkSession: Cannot use MySparkSessionExtension to configure session extensions.
java.lang.ClassNotFoundException: MySparkSessionExtension
        at java.base/java.net.URLClassLoader.findClass(URLClassLoader.java:476)
        at java.base/java.lang.ClassLoader.loadClass(ClassLoader.java:589)
        at java.base/java.lang.ClassLoader.loadClass(ClassLoader.java:522)
        at java.base/java.lang.Class.forName0(Native Method)
        at java.base/java.lang.Class.forName(Class.java:398)
        at org.apache.spark.util.Utils$.classForName(Utils.scala:216)
        at org.apache.spark.sql.SparkSession$.$anonfun$applyExtensions$1(SparkSession.scala:1194)
        at org.apache.spark.sql.SparkSession$.$anonfun$applyExtensions$1$adapted(SparkSession.scala:1192)
        at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
        at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
        at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
        at org.apache.spark.sql.SparkSession$.org$apache$spark$sql$SparkSession$$applyExtensions(SparkSession.scala:1192)
        at org.apache.spark.sql.SparkSession$Builder.getOrCreate(SparkSession.scala:956)
        at org.apache.spark.sql.hive.thriftserver.SparkSQLEnv$.init(SparkSQLEnv.scala:54)
        at org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver.<init>(SparkSQLCLIDriver.scala:328)
        at org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver$.main(SparkSQLCLIDriver.scala:160)
        at org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver.main(SparkSQLCLIDriver.scala)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.base/java.lang.reflect.Method.invoke(Method.java:566)
        at org.apache.spark.deploy.JavaMainApplication.start(SparkApplication.scala:52)
        at org.apache.spark.deploy.SparkSubmit.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:955)
        at org.apache.spark.deploy.SparkSubmit.doRunMain$1(SparkSubmit.scala:180)
        at org.apache.spark.deploy.SparkSubmit.submit(SparkSubmit.scala:203)
        at org.apache.spark.deploy.SparkSubmit.doSubmit(SparkSubmit.scala:90)
        at org.apache.spark.deploy.SparkSubmit$$anon$2.doSubmit(SparkSubmit.scala:1043)
        at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:1052)
        at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
22/05/08 11:56:44 WARN HiveConf: HiveConf of name hive.stats.jdbc.timeout does not exist
22/05/08 11:56:44 WARN HiveConf: HiveConf of name hive.stats.retries.wait does not exist
22/05/08 11:56:46 WARN ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 2.3.0
22/05/08 11:56:46 WARN ObjectStore: setMetaStoreSchemaVersion called but recording version is disabled: version = 2.3.0, comment = Set by MetaStore yu-langchu@192.168.1.142

```
