package de.mrobohm.data.column;

import de.mrobohm.data.DataType;
import de.mrobohm.data.dataset.Value;

import java.util.List;

public sealed interface ColumnConstraint permits ColumnConstraintUnique, ColumnConstraintLocalPredicate {

    // Ich denke, dass es sich hier um eine relativ wichtiges Maß handelt, um zu entscheiden, wie stark man eine Beschränkung modifizieren kann.
    // Mit gegebenen Datensätzen lassen sich härtere Beschränkungen bewerten. Für das Aufweichen kann an eine Gleichverteilung annehmen.
    double estimateRatioOfKickedValues(List<Value> values, DataType dataType);

}