package types;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.security.Key;

/**
 * TODO: Doc
 * Warning is a Tuple3<String, Long, Long>.
 * We have this class to put the Kafka serializer somewhere
 */
public class Warning {

    /**
     * Kafka Serializer
     */
    public static class Serializer implements KeyedSerializationSchema<Tuple3<String, Long, Long>> {
        @Override
        public byte[] serializeKey(Tuple3<String, Long, Long> in) {
            return null;
        }

        @Override
        public byte[] serializeValue(Tuple3<String, Long, Long> in) {
            // TODO: To json
            return String.format("{\n" +
                    "\"eventDescription\": \"%s\", \n" +
                    "\"startDate\": \"%d\", \n" +
                    "\"endDate\": \"%d\" \n" +
                    "}",
                    in.f0,
                    in.f1,
                    in.f2).getBytes();
            //return ("(" + in.f0 + ", " + in.f1 + ", " + in.f2 + ")").getBytes();
        }

        @Override
        public String getTargetTopic(Tuple3<String, Long, Long> in) {
            return null;
        }
    }
}
