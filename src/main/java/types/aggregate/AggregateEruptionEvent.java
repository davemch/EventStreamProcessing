package types.aggregate;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;
import types.socialunrest.Eruption;
import types.socialunrest.Escalation;

import java.util.Date;

public class AggregateEruptionEvent extends AggregateEvent {

    public AggregateEruptionEvent(Date startDate, Date endDate, int amount) {
        super("eruption_aggregate", startDate, endDate, amount);
    }

    /**
     * Aggregator
     */
    public static class AggregateEruptionEvents extends ProcessWindowFunction<Eruption, AggregateEruptionEvent, Long, TimeWindow> {
        @Override
        public void process(Long key, Context context, Iterable<Eruption> events, Collector<AggregateEruptionEvent> collector) throws Exception {
            int amount = 0;

            for (Eruption event : events) {
                amount++;
            }
            Date startDate = new Date(context.window().getStart());
            Date endDate = new Date(context.window().getEnd());

            collector.collect(new AggregateEruptionEvent(startDate, endDate, amount));
        }
    }

    /**
     * Kafka Serializer
     */
    public static class Serializer implements KeyedSerializationSchema<AggregateEruptionEvent> {
        @Override
        public byte[] serializeKey(AggregateEruptionEvent aggregateEruptionEvent) {
            return null;
        }

        @Override
        public byte[] serializeValue(AggregateEruptionEvent aggregateEruptionEvent) {
            return aggregateEruptionEvent.toString().getBytes();
        }

        @Override
        public String getTargetTopic(AggregateEruptionEvent aggregateEruptionEvent) {
            return null;
        }
    }
}
