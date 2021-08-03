package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class TotalTradesVolumewithEntity implements RfqMetadataExtractor {
    private long week;
    private long month;
    private long year;

    public TotalTradesVolumewithEntity(){

     this.week = DateTime.now().minusWeeks(1).getMillis();
        this.month = DateTime.now().minusMonths(1).getMillis();
        this.year = DateTime.now().minusYears(1).getMillis();
}

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        trades.createOrReplaceTempView("trade");


        String querytradesPastWeek = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(), rfq.getIsin(), new java.sql.Date(week));

        Dataset<Row> sqlQueryResultsforWeek = session.sql("SELECT SUM(LastQty) FROM Trade");
        Object volumeforWeek = sqlQueryResultsforWeek.first().get(0);

        if (volumeforWeek == null) {
            volumeforWeek = 0L;}

            String querytradesPastMonth = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                    rfq.getEntityId(), rfq.getIsin(), new java.sql.Date(month));

            Dataset<Row> sqlQueryResultsforMonth = session.sql("SELECT SUM(LastQty) FROM Trade");
            Object volumeforMonth = sqlQueryResultsforMonth.first().get(0);

            if (volumeforMonth == null) {
                volumeforMonth = 0L;}

                String querytradesPastYear = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                        rfq.getEntityId(), rfq.getIsin(), new java.sql.Date(year));

                Dataset<Row> sqlQueryResultsforYear = session.sql("SELECT SUM(LastQty) FROM Trade");
                Object volumeforYear = sqlQueryResultsforYear.first().get(0);

                if (volumeforYear == null) {
                    volumeforYear = 0L;}

                    Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
                    results.put(TotalVolumeEntitytradedforpastWeek, volumeforWeek);
                    results.put(TotalVolumeEntitytradedforpastMonth, volumeforMonth);
                    results.put(TotalVolumeEntitytradedforpastYear, volumeforYear);
                    return results;
                }
                protected void setWeek(long week)  {
                    this.week = week;}

                    protected void setMonth (long month){
                         this.month = month;}

                protected void setYear (long year) {
                    this.year = year;

        }
}



