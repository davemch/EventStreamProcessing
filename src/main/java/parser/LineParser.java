package parser;

import types.gdelt.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LineParser implements FlatMapFunction<String, Event> {

    @Override
    public void flatMap(String line, Collector<Event> collector) throws ParseException {
        String[] data = line.split("\\t");

        if (data.length != 61) {
            throw new ParseException("Amount of fields != 61", 0);
        }

        // Read each field we are interested in
        Date   date           = new SimpleDateFormat("yyyyMMdd").parse(data[1]);
        String a1CountryCode  = data[7];
        String eventCode      = data[26];
        String eventRootCode  = data[28];
        int    numMentions    = toInt(data[31]);
        double avgTone        = toDouble(data[34]);
        double a1Lat          = toDouble(data[40]);
        double a1Long         = toDouble(data[41]);

        // Create POJO wrapper...
        Event event = new Event(date, a1CountryCode, eventCode, eventRootCode, numMentions, avgTone, a1Lat, a1Long);

        // ...and send it
        collector.collect(event);
    }

    private static double toDouble(String in) {
        try {
            return in.isEmpty() ? 0 : Double.parseDouble(in);
        } catch (Exception e) {
            //System.out.println("Exception in LineParser.toDouble() for String in: " + in);
            return 0;
        }
    }

    private static int toInt(String in) {
        try {
            return in.isEmpty() ? 0 : Integer.parseInt(in);
        } catch (Exception e) {
            //System.out.println("Exception in LineParser.toInt() for String in: " + in);
            return 0;
        }
    }
}
