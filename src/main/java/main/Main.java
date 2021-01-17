package main;

import filter.Filters;
import kafka.Kafka;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import types.base.*;
import types.base.gdelt.Event;
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
                // TODO: .window(TumblingEventWindow)?????
                // ... and filter them
                .filter(new Filters.SocialUnrestFilter())
                .filter(new Filters.CountryFilter("USA"));
                // TODO: .filter(new Filters.TimeFilter(new Date()))

        // TODO: Afterwards use CEP methods to find patterns
        //eventStream.print();

        // -- Check for basic SocialUnrestEvents

        // Check for Appeal Events
        PatternStream<Event> appealPatternStream = CEP.pattern(eventStream, Appeal.getPattern());
        DataStream<Appeal> appealDataStream = appealPatternStream.select(new Appeal.Creator());
        appealDataStream.print();
        appealDataStream.addSink(kafka.appealProducer);

        // Check for Accusation Events
        PatternStream<Event> accusationPatternStream = CEP.pattern(eventStream, Accusation.getPattern());
        DataStream<Accusation> accusationDataStream = accusationPatternStream.select(new Accusation.Creator());
        accusationDataStream.print();
        accusationDataStream.addSink(kafka.accusationProducer);

        // Check for Refuse Events
        PatternStream<Event> refusePatternStream = CEP.pattern(eventStream, Refuse.getPattern());
        DataStream<Refuse> refuseDataStream = refusePatternStream.select(new Refuse.Creator());
        refuseDataStream.print();
        refuseDataStream.addSink(kafka.refuseProducer);

        //refuseDataStream
        //        .keyBy()
        //        .window()
        //        .process(new AggregateRefuses.AggregateRefusesPerDay());

        // Check for Escalation Events
        PatternStream<Event> escalationPatternStream = CEP.pattern(eventStream, Escalation.getPattern());
        DataStream<Escalation> escalationDataStream = escalationPatternStream.select(new Escalation.Creator());
        escalationDataStream.print();
        escalationDataStream.addSink(kafka.escalationProducer);

        // Check for Eruption Events
        PatternStream<Event> eruptionPatternStream = CEP.pattern(eventStream, Eruption.getPattern());
        DataStream<Eruption> eruptionDataStream = eruptionPatternStream.select(new Eruption.Creator());
        eruptionDataStream.print();
        eruptionDataStream.addSink(kafka.eruptionProducer);

        // TODO: Last send to Kafka
        kafka.sink();

        // Execute environment
        env.execute("GDELT: Black-Lives-Matter");
    }
}
