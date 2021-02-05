package types.aggregate;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;
import types.socialunrest.Refuse;

import java.util.Date;

public class AggregateRefuseEvent extends AggregateEvent {

    public AggregateRefuseEvent(Date startDate, Date endDate, int amount) {
        super("refuse_aggregate", startDate, endDate, amount);
    }

    /**
     * Aggregator
     */
    public static class AggregateRefuseEvents extends ProcessWindowFunction<Refuse, AggregateRefuseEvent, Long, TimeWindow> {
        @Override
        public void process(Long key, Context context, Iterable<Refuse> events, Collector<AggregateRefuseEvent> collector) throws Exception {
            int amount = 0;

            for (Refuse event : events) {
                amount++;
            }
            Date startDate = new Date(context.window().getStart());
            Date endDate = new Date(context.window().getEnd());

            collector.collect(new AggregateRefuseEvent(startDate, endDate, amount));
        }
    }

    /**
     * Kafka Serializer
     */
    public static class Serializer implements KeyedSerializationSchema<AggregateRefuseEvent> {
        @Override
        public byte[] serializeKey(AggregateRefuseEvent aggregateRefuseEvent) {
            return null;
        }

        @Override
        public byte[] serializeValue(AggregateRefuseEvent aggregateRefuseEvent) {
            return aggregateRefuseEvent.toString().getBytes();
        }

        @Override
        public String getTargetTopic(AggregateRefuseEvent aggregateRefuseEvent) {
            return null;
        }
    }
}
