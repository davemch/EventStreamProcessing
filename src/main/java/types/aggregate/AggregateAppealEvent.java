package types.aggregate;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;
import types.socialunrest.Appeal;

import java.util.Date;

public class AggregateAppealEvent extends AggregateEvent {

    public AggregateAppealEvent(Date startDate, Date endDate, int amount) {
        super("appeal_aggregate", startDate, endDate, amount);
    }

    /**
     * Aggregator
     */
    public static class AggregateAppealEvents extends ProcessWindowFunction<Appeal, AggregateAppealEvent, Tuple2<String, Date>, TimeWindow> {
        @Override
        public void process(Tuple2<String, Date> key, Context context, Iterable<Appeal> events, Collector<AggregateAppealEvent> collector) throws Exception {
            int amount = 0;

            for (Appeal event : events) {
                amount++;
            }
            Date startDate = new Date(context.window().getStart());
            Date endDate = new Date(context.window().getEnd());

            collector.collect(new AggregateAppealEvent(startDate, endDate, amount));
        }
    }

    /**
     * Kafka Serializer
     */
    public static class Serializer implements KeyedSerializationSchema<AggregateAppealEvent> {
        @Override
        public byte[] serializeKey(AggregateAppealEvent aggregateAppealEvent) {
            return null;
        }

        @Override
        public byte[] serializeValue(AggregateAppealEvent aggregateAppealEvent) {
            return aggregateAppealEvent.toString().getBytes();
        }

        @Override
        public String getTargetTopic(AggregateAppealEvent aggregateAppealEvent) {
            return null;
        }
    }
}
