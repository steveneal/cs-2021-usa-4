package com.cs.rfq.decorator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;

public class RfqDecoratorMain {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local[4]");


    //Spark Configuration
        SparkConf conf = new SparkConf().setAppName("RFQStreaming").setMaster("local");

        //Spark streaming context
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        //Spark Session
        SparkSession session = SparkSession.builder()
                .appName("RFQStreaming")
                .config(conf)
                .getOrCreate();

        //TODO: create a new RfqProcessor and set it listening for incoming RFQs
        RfqProcessor r = new RfqProcessor(session, jssc);
        r.startSocketListener();


        jssc.start();
        jssc.awaitTermination();


    }

}
