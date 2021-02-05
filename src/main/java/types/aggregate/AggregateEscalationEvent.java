package types.aggregate;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;
import types.socialunrest.Escalation;

import java.util.Date;

public class AggregateEscalationEvent extends AggregateEvent {

    public AggregateEscalationEvent(Date startDate, Date endDate, int amount) {
        super("escalation_aggregate", startDate, endDate, amount);
    }

    /**
     * Aggregator
     */
    public static class AggregateEscalationEvents extends ProcessWindowFunction<Escalation, AggregateEscalationEvent, Long, TimeWindow> {
        @Override
        public void process(Long key, Context context, Iterable<Escalation> events, Collector<AggregateEscalationEvent> collector) throws Exception {
            int amount = 0;

            for (Escalation event : events) {
                amount++;
            }
            Date startDate = new Date(context.window().getStart());
            Date endDate = new Date(context.window().getEnd());

            collector.collect(new AggregateEscalationEvent(startDate, endDate, amount));
        }
    }

    /**
     * Kafka Serializer
     */
    public static class Serializer implements KeyedSerializationSchema<AggregateEscalationEvent> {
        @Override
        public byte[] serializeKey(AggregateEscalationEvent aggregateEscalationEvent) {
            return null;
        }

        @Override
        public byte[] serializeValue(AggregateEscalationEvent aggregateEscalationEvent) {
            return aggregateEscalationEvent.toString().getBytes();
        }

        @Override
        public String getTargetTopic(AggregateEscalationEvent aggregateEscalationEvent) {
            return null;
        }
    }
}
