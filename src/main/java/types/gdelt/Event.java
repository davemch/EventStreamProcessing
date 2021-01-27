package types.gdelt;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Event is a POJO wrapper for GDELT events read from csv-files.
 */
public class Event {
    private final Date   date;
    private final String a1CountryCode;
    private final String eventCode;
    private final String eventRootCode;
    private final int    numMentions;
    private final double avgTone;
    private final double a1Lat;
    private final double a1Long;

    public Date getDate() {
        return date;
    }

    public String getA1CountryCode() { return a1CountryCode; }

    public String getEventCode() {
        return eventCode;
    }

    public String getEventRootCode() {
        return eventRootCode;
    }

    public int getNumMentions() {
        return numMentions;
    }

    public double getAvgTone() {
        return avgTone;
    }

    public double getA1Lat() {
        return a1Lat;
    }

    public double getA1Long() {
        return a1Long;
    }

    public Event(
            Date   date,
            String a1CountryCode,
            String eventCode,
            String eventRootCode,
            int    numMentions,
            double avgTone,
            double a1Lat,
            double a1Long
    ) {
        this.date = date;
        this.a1CountryCode = a1CountryCode;
        this.eventRootCode = eventRootCode;
        this.eventCode = eventCode;
        this.numMentions = numMentions;
        this.avgTone = avgTone;
        this.a1Lat = a1Lat;
        this.a1Long = a1Long;
    }

    /**
     * Function to extract the time stamp from an event
     */
    public static class ExtractTimestamp extends AscendingTimestampExtractor<Event> {
        @Override
        public long extractAscendingTimestamp(Event element) {
            return element.getDate().getTime();
        }
    }

    @Override
    public String toString() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return "(" +
                sdf.format(date) +
                ", " + a1CountryCode +
                ", " + eventCode +
                ", " + eventRootCode +
                ", " + numMentions +
                ", " + avgTone +
                ", " + a1Lat +
                ", " + a1Long +
                ')';
    }

}


