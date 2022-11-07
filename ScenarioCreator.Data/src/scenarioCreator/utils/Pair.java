package scenarioCreator.utils;

import org.jetbrains.annotations.NotNull;

public record Pair<T, U>(T first, U second) implements Comparable<Pair<T, U>> {
    @Override
    public int compareTo(@NotNull Pair<T, U> otherPair) {
        return this.toString().compareTo(otherPair.toString());
    }
}
