package de.mrobohm.data.column.nesting;

import de.mrobohm.data.column.constraint.ColumnConstraint;

import java.util.Set;

public sealed interface Column permits ColumnCollection, ColumnLeaf, ColumnNode {
    int id();

    String name();

    Set<ColumnConstraint> constraintSet();
}