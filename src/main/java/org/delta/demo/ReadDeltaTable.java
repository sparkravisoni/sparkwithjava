package org.delta.demo;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class ReadDeltaTable {

    public static void main(String[] args) {
        SparkConn sparkCon = new SparkConn();
        SparkSession spark = sparkCon.createDeltaSparkSession("Delta_Demo");

        // Path for Delta table
        String empDeltaFilePath = "src/main/target_database/emp_dim_delta_tbl";

        DeltaTable deltaTable = DeltaTable.forPath(empDeltaFilePath);

        deltaTable.history().show(false);

        Dataset<Row> version0 = spark.read().format("delta").option("versionAsOf", 0).load(empDeltaFilePath);
        Dataset<Row> version1 = spark.read().format("delta").option("versionAsOf", 1).load(empDeltaFilePath);

        System.out.println("Data in Version 0 (At Creation)----->");
        version0.filter(functions.col("id").isin(1,2,3,4,5,6,100,101,102)).show(false);
        System.out.println("Data in Version 1 (After Merge) ----->");
        version1.filter(functions.col("id").isin(1,2,3,4,5,6,100,101,102)).show(false);
    }
}