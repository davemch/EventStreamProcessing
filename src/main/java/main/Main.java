package main;

import filter.Filters;
import types.SimpleEvent;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import parser.LineParser;
import sources.ZipLoader;

/**
 * Parameters to run:
 * --input ./src/main/resources/black-lives-matter
 */
public class Main {
    public static void main(String[] args) throws Exception {

        // Parse command line arguments
        final ParameterTool parameters = ParameterTool.fromArgs(args);
        String filesDirectory = parameters.get("input");

        // Setup execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Call ZipLoader to stream GDELTs .zip-files content line by line
        DataStream<String> rawLine = env.addSource(new ZipLoader(filesDirectory));

        // Parse GDELTs csv-line to the SimpleEvent POJO-type
        DataStream<SimpleEvent> eventStream = rawLine
                // TODO: Refine Parser and SimpleEvent
                .flatMap(new LineParser())
                .assignTimestampsAndWatermarks(new SimpleEvent.ExtractTimestamp());

        //
        // TODO: First filter GDELT events
        //

        // Filter events
        eventStream
                .filter(new Filters.SocialUnrestFilter())
                .filter(new Filters.CountryFilter("USA"))
                .print();

        // Execute environment
        env.execute("GDELT: Black-Lives-Matter");

        //
        // TODO: Afterwards use CEP methods to find patterns
        //


        //
        // TODO: Last send to Kafka
        //
    }


}
