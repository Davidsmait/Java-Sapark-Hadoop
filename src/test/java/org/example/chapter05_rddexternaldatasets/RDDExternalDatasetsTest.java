package org.example.chapter05_rddexternaldatasets;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
}