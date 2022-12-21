object DataFrameHiveMySQLAvroParquet {




  /*

  val hiveContext = new org.apache.spark.sql.hive.HiveContext(spark.sparkContext)
  val hiveDF = hiveContext.sql(“select * from emp”)

  val df_mysql = spark.read.format(“jdbc”)
     .option(“url”, “jdbc:mysql://localhost:port/db”)
     .option(“driver”, “com.mysql.jdbc.Driver”)
     .option(“dbtable”, “tablename”)
     .option(“user”, “user”)
     .option(“password”, “password”)
     .load()

     val df_db2 = spark.read.format(“jdbc”)
     .option(“url”, “jdbc:db2://localhost:50000/dbname”)
     .option(“driver”, “com.ibm.db2.jcc.DB2Driver”)
     .option(“dbtable”, “tablename”)
     .option(“user”, “user”)
     .option(“password”, “password”)
     .load()

//org.apache.spark.sql.execution.datasources.hbase” from Hortonworks or use “org.apache.hadoop.hbase.spark
val hbaseDF = sparkSession.read
        .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()


  df2.write.format("avro")
        .mode(SaveMode.Overwrite)
        .save("\tmp\spark_out\avro\persons.avro")
  df2.write.partitionBy("_id")
          .format("avro").save("persons_partition.avro")



  df2.write
        .parquet("\tmp\spark_output\parquet\persons.parquet")
  df2.write
        .partitionBy("_id")
        .parquet("\tmp\spark_output\parquet\persons_partition.parquet")
   */

}
