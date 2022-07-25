package de.mrobohm.data.dataset;

import org.jetbrains.annotations.NotNull;

public record Value(String content) implements Comparable<Value> {
    @Override
    public int compareTo(@NotNull Value v) {
        return this.toString().compareTo(v.toString());
    }
}
