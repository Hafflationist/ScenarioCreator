package scenarioCreator.generation.processing.transformations.structural;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.column.nesting.ColumnCollection;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.table.Table;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.transformations.TableTransformation;
import scenarioCreator.generation.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import scenarioCreator.generation.processing.transformations.structural.base.NewTableBase;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.SSet;
import scenarioCreator.utils.StreamExtensions;

import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ColumnCollectionToTable implements TableTransformation {
    @Override
    public boolean conservesFlatRelations() {
        return false;
    }

    @Override
    public boolean breaksSemanticSaturation() {
        return true;
    }

    @Override
    @NotNull
    public Pair<SortedSet<Table>, List<TupleGeneratingDependency>> transform(Table table, Function<Integer, Id[]> idGenerator, Random random) {
        final var exception = new TransformationCouldNotBeExecutedException("Given table does not contain a collection as column!");

        final var columnCollectionList = table.columnList().stream()
                .filter(c -> c instanceof ColumnCollection)
                .toList();
        final var column = StreamExtensions.pickRandomOrThrow(columnCollectionList.stream(), exception, random);
        if (!(column instanceof ColumnCollection collection)) {
            throw new RuntimeException("Should never happen");
        }

        final var newIdArray = idGenerator.apply(3);
        final var newIds = new NewTableBase.NewIds(newIdArray[0], newIdArray[1], newIdArray[2]);
        final var newTable = NewTableBase.createNewTable(
                table, column.name(), collection.columnList(), newIds, false
        ).second();
        final var modifiedTable = NewTableBase.createModifiedTable(table, column, newIds, false).second();
        final var newTableSet = SSet.of(newTable, modifiedTable);
        final List<TupleGeneratingDependency> tgdList = List.of(); // TODO(nosql): tgds
        return new Pair<>(newTableSet, tgdList);
    }

    @Override
    @NotNull
    public SortedSet<Table> getCandidates(SortedSet<Table> tableSet) {
        return tableSet.stream().filter(this::hasColumnCollection).collect(Collectors.toCollection(TreeSet::new));
    }

    private boolean hasColumnCollection(Table table) {
        return table.columnList().stream().anyMatch(c -> c instanceof ColumnCollection);
    }
}
