package org.example.chapter05_rddexternaldatasets;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;

public class RDDExternalDatasetsTest {

    SparkConf sparkConf = new SparkConf().setAppName("RDDExternalDatasetsTest")
            .setMaster("local[*]");

    @ParameterizedTest
    @ValueSource(strings = {
            "src/test/resources/1000words.txt",
            "src/test/resources/wordslist.txt.gz"
    })
    @DisplayName("Test loading local text files into Spark RDD")
    void testLoadingLocalTextFileIntoSparkRDD(final String localFilePath) {

        try (final var sparkContext = new JavaSparkContext(sparkConf)){
            JavaRDD<String> myRdd = sparkContext.textFile(localFilePath);

            System.out.printf("Total lines in the file: %d%n", myRdd.count());
            System.out.println("Printing first 10 lines->");
            myRdd.take(10).forEach(System.out::println);
            System.out.println("-------------\n");
        }
    }

    @ParameterizedTest
    @MethodSource("getFilePaths")
    @DisplayName("Test loading local text files into Spark RDD Using Method Source")
    void testLoadingLocalTextFileIntoSparkRDDUsingMethodSource(final String localFilePath) {

        try (final var sparkContext = new JavaSparkContext(sparkConf)){
            JavaRDD<String> myRdd = sparkContext.textFile(localFilePath);

            System.out.printf("Total lines in the file: %d%n", myRdd.count());
            System.out.println("Printing first 10 lines->");
            myRdd.take(10).forEach(System.out::println);
            System.out.println("-------------\n");
        }
    }

    @Test
    @DisplayName("Test Loading Whole Directory Files Into Spark RDD")
    void testLoadingWholeDirectoryFilesIntoSparkRDD() {

        try (final var sparkContext = new JavaSparkContext(sparkConf)){
            String testDirPath = Path.of("src","test","resources").toString();
            JavaPairRDD<String,String> myRdd = sparkContext.wholeTextFiles(testDirPath);

            System.out.printf("Total number of files in directory [%s]: %d%n", testDirPath, myRdd.count());

            myRdd.collect().forEach(tuple -> {
                System.out.printf("File name: %s%n", tuple._1);
                System.out.println("------------------------");
                if (tuple._1.endsWith("properties")){
                    System.out.printf("Content of [%s]: %n", tuple._1);
                    System.out.println(tuple._2);
                }

            });

        }
    }

    @Test
    @DisplayName("Test Loading csv File Into Spark RDD")
    void testLoadingCSVFileIntoSparkRDD() {

        try (final var sparkContext = new JavaSparkContext(sparkConf)){
            String testCSVFilePath = Path.of("src","test","resources", "dma.csv").toString();
            JavaRDD<String> myRdd = sparkContext.textFile(testCSVFilePath);

            System.out.printf("Total number in csv file [%s]: %d%n", testCSVFilePath, myRdd.count());

            System.out.println("CSV headers->");
            System.out.println(myRdd.first());
//            myRdd.take(1).forEach(System.out::println);
            System.out.println("-------------\n");

            System.out.println("Printing first 10 lines ->");
            myRdd.take(10).forEach(System.out::println);
            System.out.println("-------------\n");


            JavaRDD<String[]> csvFields = myRdd.map(line -> line.split(","));

            csvFields.take(5)
                    .forEach(fields -> System.out.println(String.join("|", fields)));

//            System.out.println("-------------\n");
//            csvFields.take(10)
//                    .forEach(arr -> System.out.println(Arrays.toString(arr)));

        }
    }



    private static Stream<Arguments>getFilePaths(){
        return Stream.of(
                Arguments.of(Path.of("src","test","resources","1000words.txt").toString()),
                Arguments.of(Path.of("src","test","resources","wordslist.txt.gz").toString())
        );
    }

}
