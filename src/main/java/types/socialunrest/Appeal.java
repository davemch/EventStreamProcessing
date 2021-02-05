package types.socialunrest;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import types.gdelt.Event;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class Appeal extends SocialUnrestEvent {

    public Appeal(String eventCode, Date date, int numMentions,
                  double a1Lat, double a1Long, double avgTone) {
        super("appeal", eventCode, date, numMentions, a1Lat, a1Long, avgTone);
    }

    @Override
    public String toString() {
        return super.toString();
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
     * CEP Pattern
     */
    public static Pattern<Event, ?> getPattern() {
        return Pattern
                .<Event>begin("first")
                .where(new IterativeCondition<Event>() {
                    @Override
                    public boolean filter(Event event, Context<Event> context) throws Exception {
                        return event.getEventCode().equals("010");
                    }
                });
    }

    /**
     * Kafka Serializer
     */
    public static class Serializer implements KeyedSerializationSchema<Appeal> {
        @Override
        public byte[] serializeKey(Appeal appeal) {
            return null;
        }

        @Override
        public byte[] serializeValue(Appeal appeal) {
            return appeal.toString().getBytes();
        }

        @Override
        public String getTargetTopic(Appeal appeal) {
            return null;
        }
    }

    /**
     * Creator
     */
    public static class Creator implements PatternSelectFunction<Event, Appeal> {
        @Override
        public Appeal select(Map<String, List<Event>> map) throws Exception {
            Event first = map.get("first").get(0);

            return new Appeal(first.getEventCode(), first.getDate(), first.getNumMentions(),
                    first.getA1Lat(), first.getA1Long(), first.getAvgTone());
        }
    }

    /**
     * Function to define the eventDescription and the date as key
     */
    public static class AppealKeySelector implements KeySelector<Appeal, Long> {

        @Override
        public Long getKey(Appeal value) throws Exception {
            return value.date.getTime();
        }
    }
}
