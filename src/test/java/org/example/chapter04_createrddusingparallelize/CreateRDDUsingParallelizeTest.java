package org.example.chapter04_createrddusingparallelize;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CreateRDDUsingParallelizeTest {

    SparkConf sparkConf = new SparkConf().setAppName("CreateRDDUsingParallelizeTest")
                                        .setMaster("local[*]");

    @Test
    @DisplayName("Create an empty RDD with no partitions in Spark")
    void createAnEmptyRDDWithNoPartitionsInSpark(){

        try(final var sparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<Object> emptyRDD = sparkContext.emptyRDD();
            System.out.println(emptyRDD);
            System.out.printf("Number of partitions: %d%n", emptyRDD.getNumPartitions() );
        }
    }

    @Test
    @DisplayName("Create an default RDD with no partitions in Spark")
    void createAnEmptyRDDWithDefaultPartitionsInSpark(){

        try(final var sparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<Object> defaultRDD = sparkContext.parallelize(List.of("1", "2"));
            System.out.println(defaultRDD);
            System.out.printf("Number of partitions: %d%n", defaultRDD.getNumPartitions() );
        }
    }
}
