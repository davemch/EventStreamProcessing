package types.aggregate;

import java.util.Arrays;
import java.util.Date;

public abstract class AggregateEvent {
    final String eventDescription;
    final Date startDate;
    final Date endDate;
    final int amount;

    public AggregateEvent(String eventDescription, Date startDate, Date endDate, int amount) {
        this.eventDescription = eventDescription;
        this.startDate = startDate;
        this.endDate = endDate;
        this.amount = amount;
    }

    public int getAmount() {
        return this.amount;
    }

    public Date getDate() {
        return this.startDate;
    }

    @Override
    public String toString() {
        return String.format("{\n" +
                        "\"eventDescription\": \"%s\", \n" +
                        "\"startDate\": \"%s\", \n" +
                        "\"endDate\": \"%s\", \n" +
                        "\"amount\": \"%d\" \n" +
                        "}",
                eventDescription,
                startDate,
                endDate.getTime(),
                amount);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(this.toString().getBytes());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AggregateEvent) {
            return this.hashCode() == obj.hashCode();
        } else {
            return false;
        }
    }
}
