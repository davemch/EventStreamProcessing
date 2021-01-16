package types.base;

import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import types.base.gdelt.Event;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class Accusation extends SocialUnrestEvent {

    public Accusation(String eventCode, Date date, int numMentions,
                      double a1Lat, double a1Long, double avgTone) {
        super("accusation", eventCode, date, numMentions, a1Lat, a1Long, avgTone);
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
                        return event.getEventCode().equals("011");
                    }
                });
    }

    /**
     * Kafka Serializer
     */
    public static class Serializer implements KeyedSerializationSchema<Accusation> {
        @Override
        public byte[] serializeKey(Accusation accusation) {
            return null;
        }

        @Override
        public byte[] serializeValue(Accusation accusation) {
            return accusation.toString().getBytes();
        }

        @Override
        public String getTargetTopic(Accusation accusation) {
            return null;
        }
    }

    /**
     * Creator
     */
    public static class Creator implements PatternSelectFunction<Event, Accusation> {
        @Override
        public Accusation select(Map<String, List<Event>> map) throws Exception {
            Event first = map.get("first").get(0);

            return new Accusation(first.getEventCode(), first.getDate(), first.getNumMentions(),
                    first.getA1Lat(), first.getA1Long(), first.getAvgTone());
        }
    }
}