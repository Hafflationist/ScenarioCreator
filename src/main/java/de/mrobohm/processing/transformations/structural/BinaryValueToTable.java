package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.Language;
import de.mrobohm.data.Schema;
import de.mrobohm.data.column.constraint.*;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnCollection;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.column.nesting.ColumnNode;
import de.mrobohm.data.identification.Id;
import de.mrobohm.data.identification.IdPart;
import de.mrobohm.data.identification.MergeOrSplitType;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.table.Table;
import de.mrobohm.processing.integrity.IdentificationNumberCalculator;
import de.mrobohm.processing.transformations.SchemaTransformation;
import de.mrobohm.processing.transformations.constraintBased.base.FunctionalDependencyManager;
import de.mrobohm.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.processing.transformations.linguistic.helpers.LinguisticUtils;
import de.mrobohm.processing.transformations.structural.base.IdTranslation;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;

public class BinaryValueToTable implements SchemaTransformation {
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
    public Schema transform(Schema schema, Random random) {
        final var tcnbee = new TransformationCouldNotBeExecutedException("Table did not have columns with two or three values!!");
        final var validTableStream = schema.tableSet().stream().filter(this::isTableValid);
        final var table = StreamExtensions.pickRandomOrThrow(validTableStream, tcnbee, random);
        final var validColumnStream = table.columnList().stream()
                .filter(column -> column instanceof ColumnLeaf)
                .map(column -> (ColumnLeaf) column)
                .filter(leaf -> leaf.valueSet().size() == 2 || leaf.valueSet().size() == 3);
        final var chosenSplitLeaf = StreamExtensions.pickRandomOrThrow(validColumnStream, tcnbee, random);
        final var newColumnList = table.columnList().stream()
                .filter(column -> !column.equals(chosenSplitLeaf))
                .toList();
        final var tableWithoutChosenSplitLeaf = table.withColumnList(newColumnList);
        final var valueList = chosenSplitLeaf.valueSet().stream().toList();
        final var tableSplitResult = splitTable(tableWithoutChosenSplitLeaf, valueList.size());
        final var addTableStream = StreamExtensions
                .zip(
                        tableSplitResult.tableSet.stream(),
                        valueList.stream(),
                        (t, value) -> appendToName(t, value.content(), random)
                );
        final var newTableSet = StreamExtensions.replaceInStream(
                schema.tableSet().stream(),
                table,
                addTableStream
        ).collect(Collectors.toCollection(TreeSet::new));
        return IdTranslation.translateConstraints(
                schema.withTableSet(newTableSet),
                tableSplitResult.idMap,
                Set.of(chosenSplitLeaf.id())
        );
    }

    private TableSplitResult splitTable(Table table, int n) {
        final var tableSet = Stream.iterate(0, x -> x + 1)
                .limit(n)
                .map(x -> splitTablePart(table, x))
                .collect(Collectors.toCollection(TreeSet::new));

        final var idMapWrongType = tableSet.stream()
                .flatMap(this::getAllColumnIdPartStream)
                .collect(groupingBy(IdPart::predecessorId));
        final var idMap = idMapWrongType.keySet().stream()
                .collect(Collectors.toMap(id -> id, id -> (SortedSet<Id>) new TreeSet<Id>(idMapWrongType.get(id))));
        return new TableSplitResult(tableSet, idMap);
    }

    private Stream<IdPart> getAllColumnIdPartStream(Table table) {
        return table.columnList().stream()
                .flatMap(column -> IdentificationNumberCalculator.columnToIdStream(column, false))
                .filter(id -> id instanceof IdPart)
                .map(id -> (IdPart) id);
    }

    private Table splitTablePart(Table table, int partNum) {
        final var errorMsg = "Chosen table contains invalid constraints! This should be prevented by <getCandidates>!";
        final var validationException = new RuntimeException(errorMsg);
        final var newColumnList = table.columnList().stream()
                .map(column -> {
                    final var newId = new IdPart(column.id(), partNum, MergeOrSplitType.Xor);
                    final var newConstraintSet = column.constraintSet().stream().map(c -> switch (c) {
                        case ColumnConstraintForeignKey ccfk -> ccfk;
                        case ColumnConstraintForeignKeyInverse ignore -> throw validationException;
                        case ColumnConstraintUnique ccu -> {
                            final var newConstraintId = new IdPart(ccu.getUniqueGroupId(), partNum, MergeOrSplitType.Xor);
                            yield ccu.withUniqueGroupId(newConstraintId);
                        }
                        case ColumnConstraintCheckNumerical cccn -> cccn;
                        case ColumnConstraintCheckRegex cccr -> cccr;
                    }).collect(Collectors.toCollection(TreeSet::new));
                    return (Column) switch (column) {
                        case ColumnLeaf leaf -> leaf
                                .withId(newId)
                                .withConstraintSet(newConstraintSet);
                        case ColumnNode node -> node
                                .withId(newId)
                                .withConstraintSet(newConstraintSet);
                        case ColumnCollection col -> col
                                .withId(newId)
                                .withConstraintSet(newConstraintSet);
                    };
                }).toList();
        final var newFdSet = FunctionalDependencyManager.getValidFdSet(table.functionalDependencySet(), newColumnList);
        return table
                .withId(new IdPart(table.id(), partNum, MergeOrSplitType.Xor))
                .withColumnList(newColumnList)
                .withFunctionalDependencySet(newFdSet);
    }

    private Table appendToName(Table table, String suffix, Random random) {
        final var suffixPlus = (StringPlus) new StringPlusNaked(suffix, Language.Technical);
        final var newName = LinguisticUtils.merge(table.name(), suffixPlus, random);
        return table.withName(newName);
    }

    // block foreign key constraints!
    private boolean isTableValid(Table table) {

        final var enoughNonPrimKeyColumns = table.columnList().stream()
                .anyMatch(column -> !column.containsConstraint(ColumnConstraintPrimaryKey.class));

        final var enoughColumns = table.columnList().size() >= 2;

        final var isReferenced = table.columnList().stream()
                .anyMatch(column -> column.containsConstraint(ColumnConstraintForeignKeyInverse.class));

        final var splitColumnPresent = table.columnList().stream()
                .filter(column -> !column.containsConstraint(ColumnConstraintPrimaryKey.class))
                .filter(column -> !column.containsConstraint(ColumnConstraintForeignKey.class))
                .filter(column -> !column.containsConstraint(ColumnConstraintForeignKeyInverse.class))
                .filter(column -> column instanceof ColumnLeaf)
                .map(column -> (ColumnLeaf) column)
                .map(ColumnLeaf::valueSet)
                .map(Set::size)
                .anyMatch(size -> size == 2 || size == 3);

        return enoughNonPrimKeyColumns
                && enoughColumns
                && !isReferenced
                && splitColumnPresent;
    }

    @Override
    public boolean isExecutable(Schema schema) {
        return schema.tableSet().stream()
                .anyMatch(this::isTableValid);
    }

    private record TableSplitResult(SortedSet<Table> tableSet, Map<Id, SortedSet<Id>> idMap) {
    }
}