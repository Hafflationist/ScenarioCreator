package de.mrobohm.data.column.constraint;

import de.mrobohm.data.column.DataType;
import de.mrobohm.data.dataset.Value;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public sealed interface ColumnConstraint extends Comparable<ColumnConstraint>
        permits
        ColumnConstraintCheckNumerical,
        ColumnConstraintCheckRegex,
        ColumnConstraintForeignKey,
        ColumnConstraintForeignKeyInverse,
        ColumnConstraintUnique {

    // Ich denke, dass es sich hier um ein relativ wichtiges Maß handelt, um zu entscheiden, wie stark man eine Beschränkung modifizieren kann.
    // Mit gegebenen Datensätzen lassen sich härtere Beschränkungen bewerten. Für das Aufweichen kann an eine Gleichverteilung annehmen.
    double estimateRatioOfKickedValues(List<Value> values, DataType dataType);

    @Override
    default int compareTo(@NotNull ColumnConstraint cc) {
        return this.toString().compareTo(cc.toString());
    }
}