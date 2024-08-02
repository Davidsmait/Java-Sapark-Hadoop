package org.example.chapter04_createrddusingparallelize;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    @Test
    @DisplayName("Create a Spark RDD from Java Collection using parallelize method")
    void createSparkRDDUsingParallelizeMethod(){

        try(final var sparkContext = new JavaSparkContext(sparkConf)) {
            List<Integer> data = Stream.iterate(1, n -> n + 1)
                    .limit(8L)
                    .collect(Collectors.toList());
            JavaRDD<Integer> myRDD = sparkContext.parallelize(data);
            System.out.println(myRDD);
            System.out.printf("Number of partitions: %d%n", myRDD.getNumPartitions() );
            System.out.printf("Total elements in RDD: %d%n", myRDD.count() );
            System.out.println("Elements of RDD:" );
            myRDD.collect().forEach(System.out::println);

            // reduce operations
            Integer max = myRDD.reduce(Integer::max);
            Integer min = myRDD.reduce(Integer::min);
            Integer sum = myRDD.reduce(Integer::sum);

            System.out.printf("Max~>%d:, Min~>%d, Sum~>%d", max, min, sum);

        }
    }

    @Test
    @DisplayName("Create a Spark RDD from Java Collection using parallelize method with given partitions")
    void createSparkRDDUsingParallelizeMethodWithGivenPartitions(){

        try(final var sparkContext = new JavaSparkContext(sparkConf)) {
            List<Integer> data = Stream.iterate(1, n -> n + 1)
                    .limit(8L)
                    .collect(Collectors.toList());
            JavaRDD<Integer> myRDD = sparkContext.parallelize(data, 8);
            System.out.println(myRDD);
            System.out.printf("Number of partitions: %d%n", myRDD.getNumPartitions() );
            System.out.printf("Total elements in RDD: %d%n", myRDD.count() );
            System.out.println("Elements of RDD:" );
            myRDD.collect().forEach(System.out::println);

            // reduce operations
            Integer max = myRDD.reduce(Integer::max);
            Integer min = myRDD.reduce(Integer::min);
            Integer sum = myRDD.reduce(Integer::sum);

            System.out.printf("Max~>%d:, Min~>%d, Sum~>%d", max, min, sum);

        }
    }


}
