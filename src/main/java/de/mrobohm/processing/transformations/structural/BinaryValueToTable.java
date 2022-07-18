package de.mrobohm.processing.transformations.structural;

import de.mrobohm.data.Language;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKey;
import de.mrobohm.data.column.constraint.ColumnConstraintForeignKeyInverse;
import de.mrobohm.data.column.constraint.ColumnConstraintLocalPredicate;
import de.mrobohm.data.column.constraint.ColumnConstraintUnique;
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
import de.mrobohm.processing.transformations.TableTransformation;
import de.mrobohm.processing.transformations.exceptions.TransformationCouldNotBeExecutedException;
import de.mrobohm.processing.transformations.linguistic.helpers.LinguisticUtils;
import de.mrobohm.processing.transformations.structural.base.GroupingColumnsBase;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.StreamExtensions;
import org.jetbrains.annotations.NotNull;

import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

// equivalent to vertical split
public class BinaryValueToTable implements TableTransformation {
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
    public Set<Table> transform(Table table, Function<Integer, Id[]> idGenerator, Random random) {
        var tcnbee = new TransformationCouldNotBeExecutedException("Table did not have columns with two values!!");
        if (!(isTableValid(table))) {
            throw tcnbee;
        }

        var validColumnStream = table.columnList().stream()
                .filter(column -> column instanceof ColumnLeaf)
                .map(column -> (ColumnLeaf) column)
                .filter(leaf -> leaf.valueSet().size() == 2);
        var chosenSplitLeaf = StreamExtensions.pickRandomOrThrow(validColumnStream, tcnbee, random);
        var newColumnList = table.columnList().stream()
                .filter(column -> !column.equals(chosenSplitLeaf))
                .toList();
        var tableWithoutChosenSplitLeaf = table.withColumnList(newColumnList);
        var tablePair = splitTable(tableWithoutChosenSplitLeaf);
        var valueList = chosenSplitLeaf.valueSet().stream().toList();
        var tableFirst = appendToName(tablePair.first(), valueList.get(0).content(), random);
        var tableSecond = appendToName(tablePair.second(), valueList.get(1).content(), random);
        return Set.of(tableFirst, tableSecond);
    }

    private Pair<Table, Table> splitTable(Table table) {
        return new Pair<>(
                splitTablePart(table, 1),
                splitTablePart(table, 2)
        );
    }

    private Table splitTablePart(Table table, int partNum) {
        var errorMsg = "Chosen table contains invalid constraints! This should be prevented by <getCandidates>!";
        var validationException = new RuntimeException(errorMsg);
        var newColumnList = table.columnList().stream()
                .map(column -> {
                    var newId = new IdPart(column.id(), partNum, MergeOrSplitType.Xor);
                    var newConstraintSet = column.constraintSet().stream().map(c -> switch (c) {
                        case ColumnConstraintForeignKey ignore -> throw validationException;
                        case ColumnConstraintForeignKeyInverse ignore -> //noinspection DuplicateBranchesInSwitch
                                throw validationException;
                        case ColumnConstraintLocalPredicate cclp -> cclp;
                        case ColumnConstraintUnique ccu -> {
                            var newConstraintId = new IdPart(ccu.getUniqueGroupId(), partNum, MergeOrSplitType.Xor);
                            yield ccu.withUniqueGroupId(newConstraintId);
                        }
                    }).collect(Collectors.toSet());
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
        return table
                .withId(new IdPart(table.id(), partNum, MergeOrSplitType.Xor))
                .withColumnList(newColumnList);
    }

    private Table appendToName(Table table, String suffix, Random random) {
        var suffixPlus = (StringPlus) new StringPlusNaked(suffix, Language.Technical);
        var newName = LinguisticUtils.merge(table.name(), suffixPlus, random);
        return table.withName(newName);
    }


    @Override
    @NotNull
    public Set<Table> getCandidates(Set<Table> tableSet) {
        return tableSet.stream()
                .filter(this::isTableValid)
                .collect(Collectors.toSet());
    }

    // block foreign key constraints!
    private boolean isTableValid(Table table) {
        return table.columnList().stream()
                .filter(this::checkConstraints)
                .filter(column -> column instanceof ColumnLeaf)
                .map(column -> (ColumnLeaf) column)
                .map(ColumnLeaf::valueSet)
                .map(Set::size)
                .anyMatch(size -> size == 2)
            && table.columnList().size() >= 2;
    }

    private boolean checkConstraints(Column column) {
        // In Wahrheit sind nur die ColumnConstraintForeignKeyInverse ein Problem, da äußere Spalten nicht auf mehrere
        // Tabellen zeigen können.
        // ColumnConstraintForeignKey stellen eigentlich kein Problem dar, da ruhig von mehreren Tabellen auf eine
        // äußere Spalte gezeigt werden kann. Dies würde die Transformation allerdings auf das ganze Schema ausweiten.
        // Dies würde Mühe bedeuten, weil man die entsprechenden ColumnConstraintForeignKeyInverse anpassen müsste.

        // Erweitere und nutz die eigene Klasse <IdTranslation>
        return column.constraintSet().stream()
                .noneMatch(c ->
                        c instanceof ColumnConstraintForeignKey
                                || c instanceof ColumnConstraintForeignKeyInverse
                );
    }
}