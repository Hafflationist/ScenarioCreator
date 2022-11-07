package scenarioCreator.data.table;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.identification.Id;

import java.util.SortedSet;

public record FunctionalDependency(SortedSet<Id> left, SortedSet<Id> right) implements Comparable<FunctionalDependency> {
    public FunctionalDependency {
        assert left.stream().noneMatch(right::contains) : "The same id cannot be on the left and the right side!";
    }

    @Override
    public int compareTo(@NotNull FunctionalDependency fd) {
        return this.toString().compareTo(fd.toString());
    }
}
