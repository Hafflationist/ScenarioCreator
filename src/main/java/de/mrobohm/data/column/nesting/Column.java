package de.mrobohm.data.column.nesting;

import de.mrobohm.data.column.constraint.ColumnConstraint;
import de.mrobohm.data.primitives.StringPlus;

import java.util.Set;

public sealed interface Column permits ColumnCollection, ColumnLeaf, ColumnNode {
    int id();

    StringPlus name();

    Set<ColumnConstraint> constraintSet();

    boolean isNullable();
}