package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AverageTradedPriceExtractorTest extends AbstractSparkUnitTest {
    private Rfq request_for_quote;
    Dataset<Row> trades;
    Dataset<Row> tradesWrong;

    @BeforeEach
    public void setup() {
        request_for_quote = new Rfq();
        request_for_quote.setEntityId(5561279226039690843L);
        request_for_quote.setIsin("AT0000A0VRQ6");
        request_for_quote.setPrice(0.0);

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        String filePathWrong = getClass().getResource("loader-test-wrong.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
        tradesWrong = new TradeDataLoader().loadTrades(session, filePathWrong);
    }

    @Test
    public void checkAverageWhenAllTradesMatch() {

        AverageTradedPriceExtractor extractor = new AverageTradedPriceExtractor();
        extractor.setSince("2018-01-01");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(request_for_quote, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.averageTradedPrice);

        assertEquals(138.4396, result);
    }

    @Test
    public void checkVolumeWhenNoTradesMatch() {

        //all test trade data are for 2018 so this will cause no matches
        AverageTradedPriceExtractor extractor = new AverageTradedPriceExtractor();
        //extractor.setSince("2020-01-01");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(request_for_quote, session, tradesWrong);

        Object result = meta.get(RfqMetadataFieldNames.averageTradedPrice);

        assertEquals(0L, result);
    }
}
