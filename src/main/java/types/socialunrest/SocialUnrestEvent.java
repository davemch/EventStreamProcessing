package types.socialunrest;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import types.gdelt.Event;

import java.util.Arrays;
import java.util.Date;

public abstract class SocialUnrestEvent {
    final String eventDescription;
    final String eventCode;
    final Date date;
    final int numMentions;
    final double a1Lat;
    final double a1Long;
    final double avgTone;

    public SocialUnrestEvent(String eventDescription, String eventCode, Date date,
                             int numMentions, double a1Lat, double a1Long, double avgTone) {
        this.eventDescription = eventDescription;
        this.eventCode = eventCode;
        this.date = date;
        this.numMentions = numMentions;
        this.a1Lat = a1Lat;
        this.a1Long  = a1Long;
        this.avgTone = avgTone;
    }

    @Override
    public String toString() {
        return String.format("{\n" +
                        "\"eventDescription\": \"%s\", \n" +
                        "\"eventCode\": \"%s\", \n" +
                        "\"date\": \"%s\", \n" +
                        "\"numMentions\": \"%d\", \n" +
                        "\"a1Lat\": \"%f\", \n" +
                        "\"a1Long\": \"%f\", \n" +
                        "\"avgTone\": \"%f\" \n" +
                        "}",
                eventDescription,
                eventCode,
                date.getTime(),
                numMentions,
                a1Lat,
                a1Long,
                avgTone);
    }


    /**
     * Returns the hash code of bytes returned by toString method.
     */
    @Override
    public int hashCode() {
        return Arrays.hashCode(this.toString().getBytes());
    };

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SocialUnrestEvent) {
            return this.hashCode() == obj.hashCode();
        } else {
            return false;
        }
    }

    /**
     * Function to define the eventDescription and the date as key
     */
    public static class SocialUnrestEventKeySelector implements KeySelector<SocialUnrestEvent, Tuple2<String, Date>> {

        @Override
        public Tuple2<String, Date> getKey(SocialUnrestEvent value) throws Exception {
            return Tuple2.of(value.eventDescription, value.date);
        }
    }
}
