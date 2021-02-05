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

public class Eruption extends SocialUnrestEvent {

    public Eruption(String eventCode, Date date, int numMentions,
                      double a1Lat, double a1Long, double avgTone) {
        super("eruption", eventCode, date, numMentions, a1Lat, a1Long, avgTone);
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
                        return event.getEventCode().equals("014");
                    }
                });
    }

    /**
     * Moving average to count eruption/etc events
     * moving average of one week and count per week and show per day
     *
     * if extreme take as reference and look what happened with other events
     *
     * what happened 1. may(before and after)
     */

    /**
     * Kafka Serializer
     */
    public static class Serializer implements KeyedSerializationSchema<Eruption> {
        @Override
        public byte[] serializeKey(Eruption eruption) {
            return null;
        }

        @Override
        public byte[] serializeValue(Eruption eruption) {
            return eruption.toString().getBytes();
        }

        @Override
        public String getTargetTopic(Eruption eruption) {
            return null;
        }
    }

    /**
     * Creator
     */
    public static class Creator implements PatternSelectFunction<Event, Eruption> {
        @Override
        public Eruption select(Map<String, List<Event>> map) throws Exception {
            Event first = map.get("first").get(0);

            return new Eruption(first.getEventCode(), first.getDate(), first.getNumMentions(),
                    first.getA1Lat(), first.getA1Long(), first.getAvgTone());
        }
    }

    /**
     * Function to define the eventDescription and the date as key
     */
    public static class EruptionKeySelector implements KeySelector<Eruption, Long> {

        @Override
        public Long getKey(Eruption value) throws Exception {
            return value.date.getTime();
        }
    }
}
