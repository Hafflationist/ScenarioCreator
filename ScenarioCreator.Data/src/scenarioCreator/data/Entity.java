package scenarioCreator.data;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.primitives.StringPlus;

public interface Entity extends Comparable<Entity> {
    StringPlus name();

    Id id();

    @Override
    default int compareTo(@NotNull Entity entity) {
        return this.toString().compareTo(entity.toString());
    }
}
