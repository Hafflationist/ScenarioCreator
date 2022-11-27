package scenarioCreator.data.table;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.identification.Id;

import java.util.Optional;
import java.util.SortedSet;

public record FunctionalDependency(SortedSet<Id> left, SortedSet<Id> right) implements Comparable<FunctionalDependency> {
    public FunctionalDependency {
        assert left != null;
        assert right != null;
        assert left.stream().noneMatch(right::contains) : "The same id cannot be on the left and the right side!";
        assert !right.isEmpty() : "The right side cannot be empty!";
    }

    public static Optional<FunctionalDependency> tryCreate(SortedSet<Id> left, SortedSet<Id> right) {
        if (left != null && right != null && left.stream().noneMatch(right::contains) && !right.isEmpty()){
           return Optional.of(new FunctionalDependency(left, right));
        }
        return Optional.empty();
    }

    @Override
    public int compareTo(@NotNull FunctionalDependency fd) {
        return this.toString().compareTo(fd.toString());
    }
}
