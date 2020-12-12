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

        // TODO: No idea why its said everywhere to be 57|58.
        if (data.length != 61) {
            throw new ParseException("Amount of fields != 61", 0);
        }

        //++ First we read each field
        //-- Column 0-24
        String globalEventId    = data[0];
        Date   date             = new SimpleDateFormat("yyyMMdd").parse(data[1]);
        // [2:5] are different time representation. We don't need them.
        // Action1
        String a1Code           = data[5];
        String a1Name           = data[6];
        String a1CountryCode    = data[7];
        String a1KnownGroupCode = data[8];
        String a1EthnicCode     = data[9];
        String a1Religion1Code  = data[10];
        String a1Religion2Code  = data[11];
        String a1Type1Code      = data[12];
        String a1Type2Code      = data[13];
        String a1Type3Code      = data[14];
        // Action2
        String a2Code           = data[15];
        String a2Name           = data[16];
        String a2CountryCode    = data[17];
        String a2KnownGroupCode = data[18];
        String a2EthnicCode     = data[19];
        String a2Religion1Code  = data[20];
        String a2Religion2Code  = data[21];
        String a2Type1Code      = data[22];
        String a2Type2Code      = data[23];
        String a2Type3Code      = data[24];

        //-- Column 25-34
        int    isRootEvent    = toInt(data[25]);
        String eventCode      = data[26];
        String eventBaseCode  = (data[27].trim().isEmpty() || data[27].equals("---")) ? "" : data[27];
        String eventRootCode  = data[28];
        int    quadClass      = toInt(data[29]);
        double goldsteinScale = toDouble(data[30]);
        int    numMentions    = toInt(data[31]);
        int    numSources     = toInt(data[32]);
        int    numArticles    = toInt(data[33]);
        double avgTone        = toDouble(data[34]);

        //-- Column 35-48
        // Action1
        String a1GeoType      = data[35];
        String a1FullName     = data[36];
        String a1GCountryCode = data[37];
        String a1Adm1Code     = data[38];
        double a1Lat          = toDouble(data[39]);
        // TODO: ^-- Most of the time this field is empty
        double a1Long         = toDouble(data[40]);
        double a1FeatureID    = toDouble(data[41]);
        // Action2
        String a2GeoType      = data[42];
        String a2FullName     = data[43];
        String a2GCountryCode = data[44];
        String a2Adm1Code     = data[45];
        double a2Lat          = toDouble(data[46]);
        double a2Long         = toDouble(data[47]);
        double a2FeatureID    = toDouble(data[48]);

        //-- Column 49-55
        String actGeoType      = data[49];
        String actFullName     = data[50];
        String actGCountryCode = data[51];
        String actAdm1Code     = data[52];
        String actLat          = data[53];
        String actLong         = data[54];
        String actFeatureID    = data[55];

        //-- Column 56-59
        String dateAdded       = data[56];
        String a1FullLocation  = data[57];
        String a2FullLocation  = data[58];
        String actFullLocation = data[59];

        //-- Column 60
        // TODO: String source = data[60]

        //++ Now we can create the SimpleEvent POJO
        SimpleEvent event = new SimpleEvent(globalEventId, date,
                a1Code, a1Name, a1CountryCode, a1KnownGroupCode, a1EthnicCode,
                a1Religion1Code, a1Religion2Code, a1Type1Code, a1Type2Code, a1Type3Code,
                a2Code, a2Name, a2CountryCode, a2KnownGroupCode, a2EthnicCode,
                a2Religion1Code, a2Religion2Code, a2Type1Code, a2Type2Code, a2Type3Code,
                isRootEvent, eventCode, eventBaseCode, eventRootCode, quadClass, goldsteinScale,
                numMentions, numSources, numArticles, avgTone,
                a1GeoType, a1FullName, a1GCountryCode, a1Adm1Code, a1Lat, a1Long, a1FeatureID,
                a2GeoType, a2FullName, a2GCountryCode, a2Adm1Code, a2Lat, a2Long, a2FeatureID,
                actGeoType, actFullName, actGCountryCode, actAdm1Code, actLat, actLong, actFeatureID,
                dateAdded,
                a1FullLocation, a2FullLocation, actFullLocation);

        //++ And send it
        collector.collect(event);
    }

    private static double toDouble(String in) {
        // TODO: Only for debugging
        System.out.println(in);
        try {
            return in.isEmpty() ? 0 : Double.parseDouble(in);
        } catch (Exception e) {
            return 0;
        }
    }

    private static int toInt(String in) {
        // TODO: Only for debugging
        System.out.println(in);
        try {
            return in.isEmpty() ? 0 : Integer.parseInt(in);
        } catch (Exception e) {
            return 0;
        }
    }
}
