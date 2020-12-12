package filter;

import org.apache.flink.api.common.functions.FilterFunction;
import types.SimpleEvent;

public class Filters {

    public static class SocialUnrestFilter implements FilterFunction<SimpleEvent> {
        int i = 0;

        @Override
        public boolean filter(SimpleEvent event) throws Exception {
            String eventCode = event.getEventRootCode();

            String[] socialUnrestCodes = {
                    "10", // Demand
                    "11", // Disapprove
                    "12", // Reject
                    "13", // Threaten
                    "14"  // Protest
            };

            for (String want : socialUnrestCodes) {
                if (eventCode.equals(want)) {
                    return true;
                }
            }
            return false;
        }
    }
}
