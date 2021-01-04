package types;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Event is a POJO wrapper for GDELT events read from csv-files.
 */
public class Event {
    // Cheers to Java's verbosity :D
    private final String globalEventId;
    private final Date   date;
    private final String a1Code;
    private final String a1Name;
    private final String a1CountryCode;
    private final String a1KnownGroupCode;
    private final String a1EthnicCode;
    private final String a1Religion1Code;
    private final String a1Religion2Code;
    private final String a1Type1Code;
    private final String a1Type2Code;
    private final String a1Type3Code;
    private final String a2Code;
    private final String a2Name;
    private final String a2CountryCode;
    private final String a2KnownGroupCode;
    private final String a2EthnicCode;
    private final String a2Religion1Code;
    private final String a2Religion2Code;
    private final String a2Type1Code;
    private final String a2Type2Code;
    private final String a2Type3Code;
    private final int    isRootEvent;
    private final String eventCode;
    private final String eventBaseCode;
    private final String eventRootCode;
    private final int    quadClass;
    private final double goldsteinScale;
    private final int    numMentions;
    private final int    numSources;
    private final int    numArticles;
    private final double avgTone;
    private final String a1GeoType;
    private final String a1FullName;
    private final String a1GCountryCode;
    private final String a1Adm1Code;
    private final double a1Lat;
    private final double a1Long;
    private final double a1FeatureID;
    private final String a2GeoType;
    private final String a2FullName;
    private final String a2GCountryCode;
    private final String a2Adm1Code;
    private final double a2Lat;
    private final double a2Long;
    private final double a2FeatureID;
    private final String actGeoType;
    private final String actFullName;
    private final String actGCountryCode;
    private final String actAdm1Code;
    private final String actLat;
    private final String actLong;
    private final String actFeatureID;
    private final String dateAdded;
    private final String a1FullLocation;
    private final String a2FullLocation;
    private final String actFullLocation;

    public String getGlobalEventId() {
        return globalEventId;
    }

    public Date getDate() {
        return date;
    }

    public String getA1Code() {
        return a1Code;
    }

    public String getA1Name() {
        return a1Name;
    }

    public String getA1CountryCode() {
        return a1CountryCode;
    }

    public String getA1KnownGroupCode() {
        return a1KnownGroupCode;
    }

    public String getA1EthnicCode() {
        return a1EthnicCode;
    }

    public String getA1Religion1Code() {
        return a1Religion1Code;
    }

    public String getA1Religion2Code() {
        return a1Religion2Code;
    }

    public String getA1Type1Code() {
        return a1Type1Code;
    }

    public String getA1Type2Code() {
        return a1Type2Code;
    }

    public String getA1Type3Code() {
        return a1Type3Code;
    }

    public String getA2Code() {
        return a2Code;
    }

    public String getA2Name() {
        return a2Name;
    }

    public String getA2CountryCode() {
        return a2CountryCode;
    }

    public String getA2KnownGroupCode() {
        return a2KnownGroupCode;
    }

    public String getA2EthnicCode() {
        return a2EthnicCode;
    }

    public String getA2Religion1Code() {
        return a2Religion1Code;
    }

    public String getA2Religion2Code() {
        return a2Religion2Code;
    }

    public String getA2Type1Code() {
        return a2Type1Code;
    }

    public String getA2Type2Code() {
        return a2Type2Code;
    }

    public String getA2Type3Code() {
        return a2Type3Code;
    }

    public int getIsRootEvent() {
        return isRootEvent;
    }

    public String getEventBaseCode() {
        return eventBaseCode;
    }

    public String getEventCode() {
        return eventCode;
    }

    public String getEventRootCode() {
        return eventRootCode;
    }

    public int getQuadClass() {
        return quadClass;
    }

    public double getGoldsteinScale() {
        return goldsteinScale;
    }

    public int getNumMentions() {
        return numMentions;
    }

    public int getNumSources() {
        return numSources;
    }

    public int getNumArticles() {
        return numArticles;
    }

    public double getAvgTone() {
        return avgTone;
    }

    public String getA1GeoType() {
        return a1GeoType;
    }

    public String getA1FullName() {
        return a1FullName;
    }

    public String getA1GCountryCode() {
        return a1GCountryCode;
    }

    public String getA1Adm1Code() {
        return a1Adm1Code;
    }

    public double getA1Lat() {
        return a1Lat;
    }

    public double getA1Long() {
        return a1Long;
    }

    public double getA1FeatureID() {
        return a1FeatureID;
    }

    public String getA2GeoType() {
        return a2GeoType;
    }

    public String getA2FullName() {
        return a2FullName;
    }

    public String getA2GCountryCode() {
        return a2GCountryCode;
    }

    public String getA2Adm1Code() {
        return a2Adm1Code;
    }

    public double getA2Lat() {
        return a2Lat;
    }

    public double getA2Long() {
        return a2Long;
    }

    public double getA2FeatureID() {
        return a2FeatureID;
    }

    public String getActGeoType() {
        return actGeoType;
    }

    public String getActFullName() {
        return actFullName;
    }

    public String getActGCountryCode() {
        return actGCountryCode;
    }

    public String getActAdm1Code() {
        return actAdm1Code;
    }

    public String getActLat() {
        return actLat;
    }

    public String getActLong() {
        return actLong;
    }

    public String getActFeatureID() {
        return actFeatureID;
    }

    public String getDateAdded() {
        return dateAdded;
    }

    public String getA1FullLocation() {
        return a1FullLocation;
    }

    public String getA2FullLocation() {
        return a2FullLocation;
    }

    public String getActFullLocation() {
        return actFullLocation;
    }

    public Event(
            String globalEventId,
            Date   date,
            String a1Code,
            String a1Name,
            String a1CountryCode,
            String a1KnownGroupCode,
            String a1EthnicCode,
            String a1Religion1Code,
            String a1Religion2Code,
            String a1Type1Code,
            String a1Type2Code,
            String a1Type3Code,
            String a2Code,
            String a2Name,
            String a2CountryCode,
            String a2KnownGroupCode,
            String a2EthnicCode,
            String a2Religion1Code,
            String a2Religion2Code,
            String a2Type1Code,
            String a2Type2Code,
            String a2Type3Code,
            int    isRootEvent,
            String eventCode,
            String eventBaseCode,
            String eventRootCode,
            int    quadClass,
            double goldsteinScale,
            int    numMentions,
            int    numSources,
            int    numArticles,
            double avgTone,
            String a1GeoType,
            String a1FullName,
            String a1GCountryCode,
            String a1Adm1Code,
            double a1Lat,
            double a1Long,
            double a1FeatureID,
            String a2GeoType,
            String a2FullName,
            String a2GCountryCode,
            String a2Adm1Code,
            double a2Lat,
            double a2Long,
            double a2FeatureID,
            String actGeoType,
            String actFullName,
            String actGCountryCode,
            String actAdm1Code,
            String actLat,
            String actLong,
            String actFeatureID,
            String dateAdded,
            String a1FullLocation,
            String a2FullLocation,
            String actFullLocation
    ) {
        this.globalEventId = globalEventId;
        this.date = date;
        this.a1Code = a1Code;
        this.a1Name = a1Name;
        this.a1CountryCode = a1CountryCode;
        this.a1KnownGroupCode = a1KnownGroupCode;
        this.a1EthnicCode = a1EthnicCode;
        this.a1Religion1Code = a1Religion1Code;
        this.a1Religion2Code = a1Religion2Code;
        this.a1Type1Code = a1Type1Code;
        this.a1Type2Code = a1Type2Code;
        this.a1Type3Code = a1Type3Code;
        this.a2Code = a2Code;
        this.a2Name = a2Name;
        this.a2CountryCode = a2CountryCode;
        this.a2KnownGroupCode = a2KnownGroupCode;
        this.a2EthnicCode = a2EthnicCode;
        this.a2Religion1Code = a2Religion1Code;
        this.a2Religion2Code = a2Religion2Code;
        this.a2Type1Code = a2Type1Code;
        this.a2Type2Code = a2Type2Code;
        this.a2Type3Code = a2Type3Code;
        this.isRootEvent = isRootEvent;
        this.eventCode = eventCode;
        this.eventBaseCode = eventBaseCode;
        this.eventRootCode = eventRootCode;
        this.quadClass = quadClass;
        this.goldsteinScale = goldsteinScale;
        this.numMentions = numMentions;
        this.numSources = numSources;
        this.numArticles = numArticles;
        this.avgTone = avgTone;
        this.a1GeoType = a1GeoType;
        this.a1FullName = a1FullName;
        this.a1GCountryCode = a1GCountryCode;
        this.a1Adm1Code = a1Adm1Code;
        this.a1Lat = a1Lat;
        this.a1Long = a1Long;
        this.a1FeatureID = a1FeatureID;
        this.a2GeoType = a2GeoType;
        this.a2FullName = a2FullName;
        this.a2GCountryCode = a2GCountryCode;
        this.a2Adm1Code = a2Adm1Code;
        this.a2Lat = a2Lat;
        this.a2Long = a2Long;
        this.a2FeatureID = a2FeatureID;
        this.actGeoType = actGeoType;
        this.actFullName = actFullName;
        this.actGCountryCode = actGCountryCode;
        this.actAdm1Code = actAdm1Code;
        this.actLat = actLat;
        this.actLong = actLong;
        this.actFeatureID = actFeatureID;
        this.dateAdded = dateAdded;
        this.a1FullLocation = a1FullLocation;
        this.a2FullLocation = a2FullLocation;
        this.actFullLocation = actFullLocation;
    }

    @Override
    public String toString(){
        // TODO: Implement!
        return "Event: eventCode=" + this.eventCode + "; date=" + this.date.toString();
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    /**
     * Kafka Serializer
     */
    public static class Serializer implements KeyedSerializationSchema<Event> {
        @Override
        public byte[] serializeKey(Event event) {
            return null;
        }

        @Override
        public byte[] serializeValue(Event event) {
            return event.toString().getBytes();
        }

        @Override
        public String getTargetTopic(Event event) {
            return null;
        }
    }

    // TODO: Creator?

    // ------------------------------------------------------
    // ------------------------------------------------------
    // ------------------------------------------------------
    // ------------------------------------------------------
    // ------------------------------------------------------
    // ------------------------------------------------------
    // ------------------------------------------------------
    // ------------------------------------------------------

    /**
     * Function to extract the time stamp from a SimpleEvent
     */
    public static class ExtractTimestamp extends AscendingTimestampExtractor<Event> {
        @Override
        public long extractAscendingTimestamp(Event element) {
            return element.getDate().getTime();
        }
    }

    /**
     * This function select two fields from Gdelt as keys
     */
    public static class CountryAndEventCodeKeySelector implements KeySelector<Event, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> getKey(Event value) throws Exception {
            // TODO: Implement
            //return Tuple2.of(value.getActionGeo_CountryCode(), value.getEventRootCode());
            return null;
        }
    }

    // $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    // $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    // $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

    // TODO: Probably move this to other file..
    /**
     * Aggregate number of mentions per window
     * input:
     *   GDELTEventData
     * returns:
     *   Tuple4<date, country-key, event-code-key, num-mentions per window>
     */
    public static class AggregateEventsPerCountryPerDay extends ProcessWindowFunction<Event, Tuple4<String, String, String, Integer>, Tuple2<String, String>, TimeWindow> {
        @Override
        public void process(Tuple2<String, String> key, Context context, Iterable<Event> iterable, Collector<Tuple4<String, String, String, Integer>> collector) throws Exception {
            int mentions = 0;

            for (Event in: iterable) {
                mentions = mentions + in.getNumMentions();
                //System.out.println("    LINE: " + key + "  " + in.getTimeStampMs() + "  " + in.getFull_date() + "  " + date + "  " + in.getEventRootCode() + "  " + in.getNumMentions() + "  " + in.getAvgTone() + "  " + count + " " + " " + in.getActionGeo_Long() + " " + in.getActionGeo_Lat());
            }
            // a window has: context.window().getStart() and context.window().getEnd()
            // here it is return the end time of the window
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date end = new Date(context.window().getEnd());

            //Tuple4<date, country-key, event-code-key, num-mentions per window>
            Tuple4<String, String, String, Integer> aggregatedEvent =
                    new Tuple4<String, String, String, Integer>(sdf.format(end), key.f0, key.f1, mentions);
            collector.collect(aggregatedEvent);
        }
    }

}


