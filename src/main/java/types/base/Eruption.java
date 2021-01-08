package types.base;

import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import types.base.gdelt.Event;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class Eruption extends SocialUnrestEvent {

    public Eruption(String eventCode, Date date) {
        super(eventCode, date);
    }

    @Override
    public String toString() {
        return "Eruption event: eventCode=" + super.eventCode + "; date=" + super.date.toString();
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
                        return event.getEventCode().equals("014");
                    }
                });
    }

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

            return new Eruption(first.getEventCode(), first.getDate());
        }
    }
}
