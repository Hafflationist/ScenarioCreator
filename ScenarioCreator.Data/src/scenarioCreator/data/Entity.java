package scenarioCreator.data;

import scenarioCreator.data.identification.Id;
import scenarioCreator.data.primitives.StringPlus;
import org.jetbrains.annotations.NotNull;

public interface Entity extends Comparable<Entity> {
    StringPlus name();

    Id id();

    @Override
    default int compareTo(@NotNull Entity entity) {
        return this.toString().compareTo(entity.toString());
    }
}
