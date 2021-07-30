package com.cs.rfq.decorator;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.Data;
import java.awt.*;
import java.sql.Struct;

import static org.apache.spark.sql.types.DataTypes.*;

public class TradeDataLoader {

    private final static Logger log = LoggerFactory.getLogger(TradeDataLoader.class);

    public Dataset<Row> loadTrades(SparkSession session, String path) {
        //TODO: create an explicit schema for the trade data in the JSON files
        //StructType schema = null;
//        StructType schema = new StructType(new StructField[]{
//                new StructField("traderId", LongType)
//        })
//        StructType schema = new StructType().add("traderId","Long").add("entityId","Long")
//                .add("securityId", "string").add("lastQty", "Long").add("lastPx","Double")
//                .add("tradeDate","Date").add("currency","string");

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("TraderId", LongType, true),
                DataTypes.createStructField("EntityId", LongType, true),
                DataTypes.createStructField("SecurityId", StringType, true),
                DataTypes.createStructField("LastQty", LongType, true),
                DataTypes.createStructField("LastPx", DoubleType, true),
                DataTypes.createStructField("TradeDate", DateType, true),
                DataTypes.createStructField("Currency", StringType, true)
        });
        //TODO: load the trades dataset
        //Dataset<Row> trades = null;
        Dataset<Row> trades = session.read().schema(schema).json(path);
        //trades.show();

        //TODO: log a message indicating number of records loaded and the schema used
        //System.out.print(trades.count());
        trades.show(5);
        return trades;
    }

}
