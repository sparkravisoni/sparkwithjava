package org.delta.demo;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateDeltaTable {
    public static void main(String[] args)
    {
        SparkConn sparkCon = new SparkConn();
        SparkSession spark = sparkCon.createDeltaSparkSession("Delta_Demo");

        // Reading the data from CSV
        Dataset<Row> empData = spark.read()
                .option("dateFormat", "dd/MM/yyyy")
                .option("delimiter", ",")
                .option("header", "true")
                .option("inferschema", "true")
                .csv("src/main/resources/emp_data_jan1.csv");

        // Path for Delta table
        String empDeltaFilePath = "src/main/target_database/emp_dim_delta_tbl";

        // Check if the Delta table exists, then ignore
        if(DeltaTable.isDeltaTable(empDeltaFilePath))
        {
            System.out.println("Delta file already created/exists. Skipping creation!!!!");
        }
        // Check if the Delta table doesn't exist, then create a new Delta file
        else
        {
            System.out.println("Delta file creation started.....");
            empData.write().format("delta").save(empDeltaFilePath);
            System.out.println("Delta file creation successfully completed.");
        }
    }
}