package main;

import filter.Filters;
import kafka.Kafka;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import types.aggregate.*;
import types.socialunrest.*;
import types.gdelt.Event;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import parser.LineParser;
import sources.ZipLoader;

/**
 * Parameters to run:
 *   --input ./downloader/files/ (or wherever you downloaded GDELT's .zip-files to)
 */
public class Main {
    public static void main(String[] args) throws Exception {

        /**
         * TODO:
         *       * Amount of events per day
         *       * Amount of ... events per day
         *       * Moving average of events per day
         *       * Moving average of ... events per day
         */

        // Parse command line arguments
        final ParameterTool parameters = ParameterTool.fromArgs(args);
        String filesDirectory = parameters.get("input");

        // Setup execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Setup Kafka
        Kafka kafka = new Kafka("gdelt", "bootstrap.servers", "localhost:9092")
                .initProducers();

        // Call ZipLoader to stream GDELTs .zip-files content line by line
        DataStream<String> rawLine = env.addSource(new ZipLoader(filesDirectory));

        // Parse GDELTs csv-line to the SimpleEvent POJO-type...
        DataStream<Event> eventStream = rawLine
                .flatMap(new LineParser())
                .assignTimestampsAndWatermarks(new Event.ExtractTimestamp())
                // ... and filter them
                .filter(new Filters.SocialUnrestFilter())
                .filter(new Filters.CountryFilter("USA"));


        // -- Check for basic SocialUnrestEvents

        // Check for Appeal Events
        PatternStream<Event> appealPatternStream = CEP.pattern(eventStream, Appeal.getPattern());
        DataStream<Appeal> appealDataStream = appealPatternStream.select(new Appeal.Creator());
        appealDataStream.addSink(kafka.appealProducer);
        appealDataStream.print();

        // Check for Accusation Events
        PatternStream<Event> accusationPatternStream = CEP.pattern(eventStream, Accusation.getPattern());
        DataStream<Accusation> accusationDataStream = accusationPatternStream.select(new Accusation.Creator());
        accusationDataStream.addSink(kafka.accusationProducer);
        accusationDataStream.print();

        // Check for Refuse Events
        PatternStream<Event> refusePatternStream = CEP.pattern(eventStream, Refuse.getPattern());
        DataStream<Refuse> refuseDataStream = refusePatternStream.select(new Refuse.Creator());
        refuseDataStream.addSink(kafka.refuseProducer);
        refuseDataStream.print();

        // Check for Escalation Events
        PatternStream<Event> escalationPatternStream = CEP.pattern(eventStream, Escalation.getPattern());
        DataStream<Escalation> escalationDataStream = escalationPatternStream.select(new Escalation.Creator());
        escalationDataStream.addSink(kafka.escalationProducer);
        escalationDataStream.print();

        // Check for Eruption Events
        PatternStream<Event> eruptionPatternStream = CEP.pattern(eventStream, Eruption.getPattern());
        DataStream<Eruption> eruptionDataStream = eruptionPatternStream.select(new Eruption.Creator());
        eruptionDataStream.addSink(kafka.eruptionProducer);
        eruptionDataStream.print();


        // -- Calculate the amount of SocialUnrestEvents per day

        // Amount of Appeal Events
        appealDataStream
                .keyBy(new Appeal.AppealKeySelector())
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .process(new AggregateAppealEvent.AggregateAppealEvents())
                .print();

        // Amount of Accusation Events
        accusationDataStream
                .keyBy(new Accusation.AccusationKeySelector())
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .process(new AggregateAccusationEvent.AggregateAccusationEvents())
                .print();

        // Amount of Refuse Events
        refuseDataStream
                .keyBy(new Refuse.RefuseKeySelector())
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .process(new AggregateRefuseEvent.AggregateRefuseEvents())
                .print();

        // Amount of Escalation Events
        escalationDataStream
                .keyBy(new Escalation.EscalationKeySelector())
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .process(new AggregateEscalationEvent.AggregateEscalationEvents())
                .print();

        // Amount of Eruption Events
        eruptionDataStream
                .keyBy(new Eruption.EruptionKeySelector())
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .process(new AggregateEruptionEvent.AggregateEruptionEvents())
                .print();

        // Execute environment
        env.execute("GDELT: Black-Lives-Matter");
    }
}
