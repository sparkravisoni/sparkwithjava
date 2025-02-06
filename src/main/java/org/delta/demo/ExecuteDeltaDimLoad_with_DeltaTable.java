package org.delta.demo;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ExecuteDeltaDimLoad_with_DeltaTable {

    public static void main(String[] args) {
        SparkConn sparkCon = new SparkConn();
        SparkSession spark = sparkCon.createDeltaSparkSession("Delta_Demo");

        // Path for Delta table
        String empDeltaFilePath = "src/main/target_database/emp_dim_delta_tbl";

        DeltaTable.forPath(empDeltaFilePath).toDF().show(500,false);

        // If the target is a Delta table, do the following
        if(DeltaTable.isDeltaTable(empDeltaFilePath))
        {
            System.out.println("Delta updates started");

            // Read the current Delta table
            DeltaTable currentEmpDeltaTbl = DeltaTable.forPath(empDeltaFilePath);

            // Read the new SOR data
            Dataset<Row> empSourceData = spark.read().option("delimiter", ",").option("header", "true").option("inferschema", "true").csv("src/main/resources/emp_data_jan3.csv");

            // Merging the data
            currentEmpDeltaTbl.as("emp")
                    .merge(empSourceData.as("emp_sor"), "emp.id = emp_sor.id")
                    .whenMatched("(emp.first_name <> emp_sor.first_name OR emp.last_name <> emp_sor.last_name OR emp.email <> emp_sor.email OR emp.gender <> emp_sor.gender OR emp.ip_address <> emp_sor.ip_address)")
                    .updateAll()
                    .whenNotMatched()
                    .insertAll()
                    .execute();

            System.out.println("Delta updates Completed.");
        }
        else System.out.println("Not a Delta table. Process Skipped!!!");
    }
}
