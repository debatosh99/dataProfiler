package com.wellsfargo.pks

import java.util.Properties

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions.udf

object simple_data_profiler {
  def main(args: Array[String]): Unit = {

    /*
    Input Parameters
    ----------------
    dprofiler_user: The rdbms username
    dprofiler_password: The rdbms password
    dprofiler_database: The rdbms database name
    dprofiler_schema: The rdbms schema name
    dprofiler_table: The rdbms table name

     */

    val Array(dprofiler_user, dprofiler_password, dprofiler_server, dprofiler_database, dprofiler_schema, dprofiler_table, dprofiler_out_loc) = args

    println("Spark simple data profiling Application Started ...")

    val RDBMS_dprofiler_user = dprofiler_user
    val RDBMS_dprofiler_password = dprofiler_password
    val RDBMS_dprofiler_server = dprofiler_server
    val RDBMS_dprofiler_database = dprofiler_database
    val RDBMS_dprofiler_schema = dprofiler_schema
    //val RDBMS_dprofiler_table = dprofiler_table
    val RDBMS_dprofiler_tables = dprofiler_table.split(",").map(_.toString).distinct
    val RDBMS_dprofiler_out_loc = dprofiler_out_loc

    System.setProperty("HADOOP_USER_NAME","hadoop")

    /*
    val spark = SparkSession.builder
        .master("local[*]")
        .appName("Data profiling Demo")
        .getOrCreate()
    */

    //db_target_properties = {"user":"debu", "password":"password", "driver":"com.mysql.jdbc.Driver"}
    //df.write.jdbc(url='jdbc:mysql://localhost:3306/ip2location',  table="transaction_geoip",  properties=db_target_properties, mode="append")

    val spark = SparkSession.builder
      .appName("Spark simple data profiler Demo")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val oracle_properties: Properties = new Properties()
    oracle_properties.setProperty("user",RDBMS_dprofiler_user)
    oracle_properties.setProperty("password",RDBMS_dprofiler_password)
    oracle_properties.setProperty("driver","oracle.jdbc.driver.OracleDriver")

    val mysql_properties: Properties = new Properties()
    mysql_properties.setProperty("user",RDBMS_dprofiler_user)
    mysql_properties.setProperty("password",RDBMS_dprofiler_password)
    mysql_properties.setProperty("driver","com.mysql.jdbc.Driver")

    val RDBMS_dprofiler_single_table = ""

    for (RDBMS_dprofiler_single_table <- RDBMS_dprofiler_tables) {
     // val rdbms_df_in = spark.read.jdbc(url = "jdbc:Oracle:thin:@//" + RDBMS_dprofiler_server + "/" + RDBMS_dprofiler_database, table = RDBMS_dprofiler_single_table, properties = oracle_properties)
    //  val rdbms_df_in = spark.read.jdbc(url = "jdbc:mysql//" + RDBMS_dprofiler_server + "/" + RDBMS_dprofiler_database, table = RDBMS_dprofiler_single_table, properties = mysql_properties)
      val rdbms_df_in = spark.read.jdbc(url = "jdbc:mysql://localhost:3306/ip2location", table = RDBMS_dprofiler_single_table, properties = mysql_properties)
      val rdbms_df_out = rdbms_df_in.summary()
      rdbms_df_out.show()
      rdbms_df_out.write.format("jdbc").mode("overwrite").option("url","jdbc:mysql://localhost:3306/ip2location").option("dbtable","db_profile1").option("driver","com.mysql.jdbc.Driver").option("user","debu").option("password","password").save()
      rdbms_df_out.coalesce(1).write.format("csv").option("header", "true").save(RDBMS_dprofiler_out_loc+"_"+RDBMS_dprofiler_single_table)
    }

  }
}
