package types;

import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class Refuse extends SocialUnrestEvent {
    private final String eventCode;
    private final Date date;

    public Refuse(String eventCode, Date date) {
        this.eventCode = eventCode;
        this.date = date;
    }

    @Override
    public String toString() {
        return "Refuse event: eventCode=" + eventCode + "; date=" + date.toString();
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
                        return event.getEventCode().equals("012");
                    }
                });
    }

    /**
     * Kafka Serializer
     */
    public static class Serializer implements KeyedSerializationSchema<Refuse> {
        @Override
        public byte[] serializeKey(Refuse refuse) {
            return null;
        }

        @Override
        public byte[] serializeValue(Refuse refuse) {
            return refuse.toString().getBytes();
        }

        @Override
        public String getTargetTopic(Refuse refuse) {
            return null;
        }
    }

    /**
     * Creator
     */
    public static class Creator implements PatternSelectFunction<Event, Refuse> {
        @Override
        public Refuse select(Map<String, List<Event>> map) throws Exception {
            Event first = map.get("first").get(0);

            return new Refuse(first.getEventCode(), first.getDate());
        }
    }
}
