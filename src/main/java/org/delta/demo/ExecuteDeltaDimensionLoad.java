package org.delta.demo;

import org.apache.spark.sql.*;

public class ExecuteDeltaDimensionLoad {
    public static void main(String[] args)
    {
        SparkConn sparkCon = new SparkConn();

        SparkSession spark = sparkCon.createSparkSession("Delta_Demo");

        // Read current target data (Step 1)
        Dataset<Row> currentEmpDim = spark.read().parquet("src/main/target_database/emp_dim.parquet");

        // Read new source data (Step 1)
        Dataset<Row> empSourceData = spark.read().option("dateFormat", "dd/MM/yyyy").option("delimiter", ",").option("header", "true").option("inferschema", "true").csv("src/main/resources/emp_data_jan3.csv");

        // Step 2: Perform Source ANTI JOIN target on key columns to get non-matching/updated keys.
        Dataset<Row> deltaEmpData = empSourceData.join(currentEmpDim,
                empSourceData.col("id").eqNullSafe(currentEmpDim.col("id"))
                        .and(empSourceData.col("first_name").eqNullSafe(currentEmpDim.col("first_name")))
                        .and(empSourceData.col("last_name").eqNullSafe(currentEmpDim.col("last_name")))
                        .and(empSourceData.col("email").eqNullSafe(currentEmpDim.col("email")))
                        .and(empSourceData.col("gender").eqNullSafe(currentEmpDim.col("gender")))
                        .and(empSourceData.col("ip_address").eqNullSafe(currentEmpDim.col("ip_address"))),
                "LEFT_ANTI")
                .withColumn("eff_dt", functions.current_date())
                .withColumn("end_dt", functions.to_date(functions.lit("9999-12-31"), "yyyy-MM-dd"));

        // Step 3 & 4: Left join the data in step 2 to get the new END_DT for already existing keys.
        //             Take data from step 2, add END_DT as high date "9999–12–31" and EFF_DT as today's date (or execution date),
        //             and then UNION it with data from step 3.
        Dataset<Row> newEmpDim = currentEmpDim.join(deltaEmpData, currentEmpDim.col("id").eqNullSafe(deltaEmpData.col("id")), "LEFT")
                .withColumn("new_end_dt", functions.when(deltaEmpData.col("id").isNull(), currentEmpDim.col("end_dt")).otherwise(functions.current_date()))
                .drop("end_dt")
                .withColumnRenamed("new_end_dt", "end_dt")
                .select(currentEmpDim.col("id"),
                        currentEmpDim.col("first_name"),
                        currentEmpDim.col("last_name"),
                        currentEmpDim.col("email"),
                        currentEmpDim.col("gender"),
                        currentEmpDim.col("ip_address"),
                        currentEmpDim.col("eff_dt"),
                        functions.col("end_dt"))
                .unionAll(deltaEmpData.select(
                        deltaEmpData.col("id"),
                        deltaEmpData.col("first_name"),
                        deltaEmpData.col("last_name"),
                        deltaEmpData.col("email"),
                        deltaEmpData.col("gender"),
                        deltaEmpData.col("ip_address"),
                        deltaEmpData.col("eff_dt"),
                        deltaEmpData.col("end_dt")));

        // Step 5: Insert data / display it for Demo ;P
        newEmpDim.orderBy(functions.col("id"), functions.col("end_dt").desc_nulls_last()).show(500, false);
    }
}