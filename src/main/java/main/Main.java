package main;

/**
 * Parameters to run (example):
 * --input ./src/main/resources/2006/ --output event-out.csv
 */
public class Main {
    public static void main(String[] args) throws Exception {

        //
        // TODO: First filter GDELT events
        //

        /*
        final ParameterTool parameters = ParameterTool.fromArgs(args);
        String gdeltFilesDirectory = parameters.get("input");
        String outputCsvFile = parameters.get("output");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Reading line by line raw data from the Event data file
        DataStream<String> gdeltRawEventData = env.addSource(new GDELTZipDataSourceFuntion(gdeltFilesDirectory));

        // Once we read the lines of every raw file, parse line by line to extract one GDELT EventData per line
        DataStream<GDELTEventData> gdeltEventData = gdeltRawEventData
                .flatMap(new ParseLineGdeltEventData())
                .assignTimestampsAndWatermarks(new ExtractTimestampGDELTEvent());

        // Processing GDELT Events
        gdeltEventData
                // In this example we are filtering Thailand country
                .filter(new CountryFilter())
                // KeyBy country and event code
                .keyBy(new CountryAndEventCodeKeySelector())
                .window(TumblingEventTimeWindows.of(Time.days(365)))
                // Aggregrating a period of time (window) per country and per Event code:
                // Tuple4<date, country-key, event-code-key, num-mentions per window>
                .process(new AggregateEventsPerCountryPerDay())

                // print on consola
                .print();

        // write result in a file
        //.writeAsCsv(outputCsvFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1);


        env.execute("GdeltEventJob");
        */

        //
        // TODO: Afterwards use CEP methods to find patterns
        //


        //
        // TODO: Last send to Kafka
        //
    }


}
