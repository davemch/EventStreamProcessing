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

public class Escalation extends SocialUnrestEvent {

    public Escalation(String eventCode, Date date, int numMentions,
                      double a1Lat, double a1Long, double avgTone) {
        super("escalation", eventCode, date, numMentions, a1Lat, a1Long, avgTone);
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
                        return event.getEventCode().equals("013");
                    }
                });
    }

    /**
     * Kafka Serializer
     */
    public static class Serializer implements KeyedSerializationSchema<Escalation> {
        @Override
        public byte[] serializeKey(Escalation escalation) {
            return null;
        }

        @Override
        public byte[] serializeValue(Escalation escalation) {
            return escalation.toString().getBytes();
        }

        @Override
        public String getTargetTopic(Escalation escalation) {
            return null;
        }
    }

    /**
     * Creator
     */
    public static class Creator implements PatternSelectFunction<Event, Escalation> {
        @Override
        public Escalation select(Map<String, List<Event>> map) throws Exception {
            Event first = map.get("first").get(0);

            return new Escalation(first.getEventCode(), first.getDate(), first.getNumMentions(),
                    first.getA1Lat(), first.getA1Long(), first.getAvgTone());
        }
    }

    /**
     * Function to define the eventDescription and the date as key
     */
    public static class EscalationKeySelector implements KeySelector<Escalation, Tuple2<String, Date>> {

        @Override
        public Tuple2<String, Date> getKey(Escalation value) throws Exception {
            return Tuple2.of(value.eventDescription, value.date);
        }
    }
}
