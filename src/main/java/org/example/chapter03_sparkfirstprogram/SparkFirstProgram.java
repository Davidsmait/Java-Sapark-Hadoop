package org.example.chapter03_sparkfirstprogram;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SparkFirstProgram {

    public static void main(String[] args) {
        try(SparkSession spark = SparkSession.builder()
                .appName("SparkFirstProgram")
                .master("local[*]")
                .getOrCreate();

            JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());){

            List<Integer> data = Stream.iterate(1, n -> n + 1)
                    .limit(5)
                    .collect(Collectors.toList());

            data.forEach(System.out::println);
            JavaRDD<Integer> myRdd = sc.parallelize(data);

            System.out.printf("Total elements in RDD: %d%n", myRdd.count());
            System.out.printf("Total elements in RDD: %d%n", myRdd.getNumPartitions());

            Integer max = myRdd.reduce(Integer::max);
            Integer min = myRdd.reduce(Integer::min);
            Integer sum = myRdd.reduce(Integer::sum);

            System.out.printf("Max~>%d:, Min~>%d, Sum~>%d", max, min, sum);

            try( Scanner scanner = new Scanner(System.in)) {
                scanner.nextLine();
            }
        }
    }
}
