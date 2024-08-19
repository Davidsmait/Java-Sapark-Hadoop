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

import java.nio.file.AccessDeniedException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;

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
    @DisplayName("Test Loading Amazon S3 File Into Spark RDD")
    void testLoadingAmazonS3FileIntoSparkRDD() {

        try (final var sparkContext = new JavaSparkContext(sparkConf)){

            // Replace key with AWS account key (can find this on IAM)
            sparkContext.hadoopConfiguration().set("fs.s3a.access.key", "AWS access-key value");

            // Replace key with AWS secret key (can find this on IAM)
            sparkContext.hadoopConfiguration().set("fs.s3a.access.key", "AWS secret-key value");

            //Read a single text file
            final var myRdd = sparkContext.textFile("s3a://backstreetbrogrammer/spark/1TrqillionWords.txt/gz");

            assertThrows(AccessDeniedException.class, myRdd::count);
        }
    }



    private static Stream<Arguments>getFilePaths(){
        return Stream.of(
                Arguments.of(Path.of("src","test","resources","1000words.txt").toString()),
                Arguments.of(Path.of("src","test","resources","wordslist.txt.gz").toString())
        );
    }

}
