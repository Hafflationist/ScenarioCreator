package de.mrobohm.processing.transformations.constraintBased.base;

import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.table.FunctionalDependency;
import de.mrobohm.processing.integrity.IdentificationNumberCalculator;

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
}
