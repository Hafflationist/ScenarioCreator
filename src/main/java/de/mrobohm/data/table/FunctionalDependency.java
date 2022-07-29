package de.mrobohm.data.table;

import de.mrobohm.data.identification.Id;

import java.util.SortedSet;

public record FunctionalDependency(SortedSet<Id> left, SortedSet<Id> right) {
    public FunctionalDependency(SortedSet<Id> left, SortedSet<Id> right) {
        assert left.stream().noneMatch(right::contains) : "The same id cannot be on the left and the right side!";
        this.left = left;
        this.right = right;
    }
}
