package types.average;

import java.util.Arrays;

public abstract class AverageEvent {
    final String eventDescription;

    public AverageEvent(String eventDescription) {
        this.eventDescription = eventDescription;
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(this.toString().getBytes());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AverageEvent) {
            return this.hashCode() == obj.hashCode();
        } else {
            return false;
        }
    }
}
