package scenarioCreator.generation.processing.transformations.structural;

import org.jetbrains.annotations.NotNull;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.table.Table;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.processing.transformations.TableTransformation;
import scenarioCreator.generation.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import scenarioCreator.generation.processing.transformations.structural.base.GroupingColumnsBase;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.SSet;
import scenarioCreator.utils.StreamExtensions;

import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public class GroupColumnLeafsToNode implements TableTransformation {
    @Override
    public boolean conservesFlatRelations() {
        return false;
    }

    @Override
    public boolean breaksSemanticSaturation() {
        return false;
    }

    @Override
    @NotNull
    public Pair<SortedSet<Table>, List<TupleGeneratingDependency>> transform(Table table, Function<Integer, Id[]> idGenerator, Random random) {
        if (!(GroupingColumnsBase.containsGroupableColumns(table.columnList()))) {
            throw new TransformationCouldNotBeExecutedException("Table did not have groupable columns!!");
        }

        final var groupableColumnList = GroupingColumnsBase.findGroupableColumns(table.columnList(), random);
        final var newIds = idGenerator.apply(1);
        final var newColumn = GroupingColumnsBase.createNewColumnNode(newIds[0], groupableColumnList, random);

        final var newColumnList = StreamExtensions.<Column>replaceInStream(
                table.columnList().stream(),
                groupableColumnList.stream(),
                (Column)newColumn).toList();

        final var newTableSet = SSet.of(table.withColumnList(newColumnList));
        final List<TupleGeneratingDependency> tgdList = List.of(); //TODO: tgds
        return new Pair<>(newTableSet, tgdList);
    }

    @Override
    @NotNull
    public SortedSet<Table> getCandidates(SortedSet<Table> tableSet) {
        return tableSet.stream()
                .filter(t -> GroupingColumnsBase.containsGroupableColumns(t.columnList()))
                .collect(Collectors.toCollection(TreeSet::new));
    }
}