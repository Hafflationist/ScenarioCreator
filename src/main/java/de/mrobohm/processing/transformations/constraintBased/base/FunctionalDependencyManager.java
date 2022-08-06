package de.mrobohm.processing.transformations.constraintBased.base;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.table.FunctionalDependency;
import de.mrobohm.processing.integrity.IdentificationNumberCalculator;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.SSet;
import one.util.streamex.StreamEx;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public final class FunctionalDependencyManager {
    private FunctionalDependencyManager() {
    }

    public static SortedSet<FunctionalDependency> getValidFdSet(
            SortedSet<FunctionalDependency> functionalDependencySet,
            List<Column> columnList
    ) {
        var allColumnIdSet = columnList.stream()
                .flatMap(column -> IdentificationNumberCalculator.columnToIdStream(column, false))
                .collect(Collectors.toSet());
        return functionalDependencySet.stream()
                .filter(fd -> allColumnIdSet.containsAll(fd.left()))
                .filter(fd -> allColumnIdSet.containsAll(fd.right()))
                .collect(Collectors.toCollection(TreeSet::new));
    }

    public static Schema transClosure(Schema schema) {
        var newTableSet = schema.tableSet().stream()
                .map(t -> {
                    var newFdSet = transClosure(t.functionalDependencySet());
                    if (newFdSet.equals(t.functionalDependencySet())) {
                        return t;
                    }
                    return t.withFunctionalDependencySet(newFdSet);
                })
                .collect(Collectors.toCollection(TreeSet::new));
        if (schema.tableSet().equals(newTableSet)) {
            return schema;
        }
        return schema.withTableSet(newTableSet);
    }

    /**
     * @param fdSet
     * @return for each fd the right-hand side is expanded to the attribute closure of the left-hand side
     */
    public static SortedSet<FunctionalDependency> transClosure(SortedSet<FunctionalDependency> fdSet) {
        return fdSet.stream()
                .map(FunctionalDependency::left)
                .map(attrSet -> new FunctionalDependency(attrSet, attributeClosure(attrSet, fdSet)))
                .collect(Collectors.toCollection(TreeSet::new));
    }
    // In Wirklichkeit brauchen wir die abgeschlossene Hülle,
    // wobei die linke und rechte Seite jeder funktionalen Abhängigkeit disjunkt sein müssen.
    // Die restlichen funktionalen Abhängigkeiten bringen hier keinen Mehrwert.

    public static SortedSet<Id> attributeClosure(SortedSet<Id> forColumnIdSet, SortedSet<FunctionalDependency> fdSet) {
        return StreamEx
                .iterate(forColumnIdSet, acc -> {
                    var newAttributes = fdSet.stream()
                            .filter(fd -> acc.containsAll(fd.left()))
                            .flatMap(fd -> fd.right().stream());
                    return SSet.concat(acc, newAttributes);
                })
                .pairMap(Pair::new)
                .findFirst(pair -> pair.first().size() == pair.second().size())
                .map(Pair::first)
                .orElseThrow();
    }
}
