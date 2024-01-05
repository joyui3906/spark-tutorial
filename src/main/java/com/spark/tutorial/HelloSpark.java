package com.spark.tutorial;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class HelloSpark {

    public static void main(String[] args) {

        String logFile = "../../../sample/logFile.txt";

        SparkSession spark = SparkSession.builder().appName("character counter").getOrCreate();

        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter((String s) -> s.contains("a")).count();
        long numBs = logData.filter((String s) -> s.contains("b")).count();


        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }
}
