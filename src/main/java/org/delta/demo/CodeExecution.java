package org.delta.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.functions.*;

public class CodeExecution {
    public static void main(String[] args)
    {
        SparkConn sparkCon = new SparkConn();
        SparkSession spark = sparkCon.createSparkSession("Delta_Demo");

        // Reading the data from CSV
        Dataset<Row> empData = spark.read()
                .option("dateFormat", "dd/MM/yyyy")
                .option("delimiter", ",")
                .option("header", "true")
                .option("inferschema", "true")
                .csv("src/main/resources/emp_data_jan1.csv")
                .withColumn("eff_dt", functions.to_date(functions.lit("2025-02-04"), "yyyy-MM-dd"))
                .withColumn("end_dt", functions.to_date(functions.lit("9999-12-31"), "yyyy-MM-dd"));

        empData.printSchema();
        empData.show(5, false);

        // Writing the target table
        empData.write().mode("overwrite").format("parquet").save("src/main/target_database/emp_dim.parquet");
    }
}