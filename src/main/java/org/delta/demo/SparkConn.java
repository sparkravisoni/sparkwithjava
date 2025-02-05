package org.delta.demo;

import org.apache.spark.sql.SparkSession;

public class SparkConn {
    public SparkSession createDeltaSparkSession(String appName)
    {
        return SparkSession.builder().appName(appName)
                .config("spark.master", "local")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();
    }

    public SparkSession createSparkSession(String appName)
    {
        return SparkSession.builder().appName(appName)
                .config("spark.master", "local")
                .getOrCreate();
    }
}