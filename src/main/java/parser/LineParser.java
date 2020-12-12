package parser;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import types.SimpleEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LineParser implements FlatMapFunction<String, SimpleEvent> {

    @Override
    public void flatMap(String line, Collector<SimpleEvent> collector) throws ParseException {
        String[] data = line.split("\\t");

        if (data.length != 61) {
            throw new ParseException("Amount of fields != 61", 0);
        }

        String eventRecordId = data[0];
        Date date = new SimpleDateFormat("yyyMMdd").parse(data[1]);
        String monthYear = data[2];
        String full_date = data[1];

        String EventBaseCode;
        if (data[27].trim().isEmpty() || data[27].equals("") || data[27].equals("---")) {
            EventBaseCode = "";
        } else {
            EventBaseCode = data[27];
        }

        String EventRootCode = data[28];
        int NumMentions = Integer.parseInt(data[31]);
        float AvgTone = Float.parseFloat(data[34]);
        String ActionGeo_Fullname = data[50];
        String ActionGeo_CountryCode =data[51];
        float GoldsteinScale;
        if (data[30].isEmpty()){
            GoldsteinScale = 0;
        } else {
            GoldsteinScale = Float.parseFloat(data[30]);
        }
        String ActionGeo_Lat,ActionGeo_Long;
        if (data[53].isEmpty()){
            ActionGeo_Lat = "";
        }else {
            ActionGeo_Lat = data[53];
        }
        if (data[54].isEmpty()){
            ActionGeo_Long = "";
        } else {
            ActionGeo_Long = data[54];
        }

        collector.collect(new SimpleEvent("gdelt_event", eventRecordId, date,full_date, monthYear, EventBaseCode,
                EventRootCode, GoldsteinScale, NumMentions, AvgTone,ActionGeo_Fullname, ActionGeo_CountryCode, ActionGeo_Lat,
                ActionGeo_Long));
    }
}
