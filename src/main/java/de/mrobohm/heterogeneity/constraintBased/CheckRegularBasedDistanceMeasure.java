package de.mrobohm.heterogeneity.constraintBased;

import de.mrobohm.data.Schema;
import de.mrobohm.data.column.constraint.ColumnConstraintCheckRegex;
import de.mrobohm.data.column.constraint.regexy.RegularExpression;
import de.mrobohm.data.column.nesting.Column;
import de.mrobohm.data.column.nesting.ColumnLeaf;
import de.mrobohm.data.identification.*;
import de.mrobohm.heterogeneity.constraintBased.regexy.DFA;
import de.mrobohm.heterogeneity.constraintBased.regexy.DfaToRandomString;
import de.mrobohm.heterogeneity.constraintBased.regexy.NfaToDfa;
import de.mrobohm.heterogeneity.constraintBased.regexy.RegexToNfa;
import de.mrobohm.processing.integrity.IdentificationNumberCalculator;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.SSet;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class CheckRegularBasedDistanceMeasure {

    private static final int SAMPLE_SIZE = 1000;

    private CheckRegularBasedDistanceMeasure() {
    }

    public static double calculateDistanceRelative(
            Schema schema1, Schema schema2, Random random
    ) {
        final var distanceAbsolute = calculateDistanceAbsolute(schema1, schema2, random);
        final var schema1Size = IdentificationNumberCalculator.getAllIds(schema1, false).count();
        final var schema2Size = IdentificationNumberCalculator.getAllIds(schema2, false).count();
        return (2.0 * distanceAbsolute) / (double) (schema1Size + schema2Size);
    }

    public static double calculateDistanceAbsolute(
            Schema schema1, Schema schema2, Random random
    ) {
        return aggregate(findCorrespondingTablePairs(schema1, schema2)).entrySet().stream()
                .mapToDouble(entry -> diffOfColumns(entry.getKey(), entry.getValue(), random))
                .sum();
    }

    private static Stream<Pair<Column, Column>> findCorrespondingTablePairs(Schema schema1, Schema schema2) {
        return BasedConstraintBasedBase
                .findCorrespondingEntityPairs(
                        extractRelevantColumns(schema1),
                        extractRelevantColumns(schema2)
                )
                .filter(pair -> isValidSplitMergeId(pair.first().id())
                        && isValidSplitMergeId(pair.second().id()));
    }

    private static Stream<Column> extractRelevantColumns(Schema schema) {
        return schema.tableSet().stream()
                .flatMap(t -> t.columnList().stream())
                .filter(column -> column.containsConstraint(ColumnConstraintCheckRegex.class));
    }

    private static boolean isValidSplitMergeId(Id id) {
        return switch (id) {
            case IdSimple ignore -> true;
            case IdMerge idm -> idm.mergeType().equals(MergeOrSplitType.Xor);
            case IdPart idp -> idp.splitType().equals(MergeOrSplitType.Xor);
        };
    }

    private static Map<Column, SortedSet<Column>> aggregate(Stream<Pair<Column, Column>> correspondence) {
        final var correspondenceList = correspondence.toList();
        final var aggregated1 = aggregate(correspondenceList, Pair::first, Pair::second);
        final var aggregated2 = aggregate(correspondenceList, Pair::second, Pair::first);
        return SSet.concat(aggregated1.keySet(), aggregated2.keySet()).stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        key -> aggregated1.getOrDefault(key, aggregated2.getOrDefault(key, SSet.of()))
                ));
    }

    private static <T> Map<Column, SortedSet<Column>> aggregate(
            List<T> correspondenceList, Function<T, Column> first, Function<T, Column> second
    ) {
        return correspondenceList.stream()
                .map(first)
                .distinct()
                .collect(Collectors.toMap(
                        Function.identity(),
                        column -> correspondenceList.stream()
                                .filter(corr -> column.id().equals(first.apply(corr).id()))
                                .map(second)
                                .collect(Collectors.toCollection(TreeSet::new)))
                );
    }

    private static double diffOfColumns(Column column, SortedSet<Column> columnSet, Random random) {
        if (!(column instanceof ColumnLeaf)) {
            return 0.0;
        }
        final var columnLeafSet = columnSet.stream()
                .filter(col -> col instanceof ColumnLeaf)
                .map(col -> (ColumnLeaf) col)
                .collect(Collectors.toCollection(TreeSet::new));
        if (columnLeafSet.isEmpty()) {
            return 0.0;
        }

        final var regex = columnToRegularExpression(column);
        return columnSet.stream()
                .mapToDouble(otherColumn -> {
                    final var otherRegex= columnToRegularExpression(otherColumn);
                    return diffOfRegularExpressions(regex, otherRegex, random);
                })
                .average()
                .orElse(0.0);
    }

    private static RegularExpression columnToRegularExpression(Column column) {
        return column.constraintSet().stream()
                .filter(c -> c instanceof ColumnConstraintCheckRegex)
                .map(c -> (ColumnConstraintCheckRegex) c)
                .map(ColumnConstraintCheckRegex::regularExpression)
                .findFirst()
                .orElse(RegularExpression.acceptsEverything());
    }

    private static double diffOfRegularExpressions(
            RegularExpression regularExpression1, RegularExpression regularExpression2, Random random
    ) {
        final var dfa1 = regexToDfa(regularExpression1);
        final var dfa2 = regexToDfa(regularExpression2);
        final var testValues = generateTestValues(dfa1, dfa2, SAMPLE_SIZE, random);
        final var acceptedBy1 = testValues.parallelStream()
                .filter(dfa1::acceptsString)
                .collect(Collectors.toSet());
        final var acceptedBy2 = testValues.parallelStream()
                .filter(dfa2::acceptsString)
                .collect(Collectors.toSet());
        final var acceptedByBoth = acceptedBy1.stream()
                .filter(acceptedBy2::contains)
                .collect(Collectors.toSet());
        final var acceptedByOne = SSet.concat(acceptedBy1, acceptedBy2);
        return 1.0 - ((double) acceptedByBoth.size() / (double) acceptedByOne.size());
    }

    private static DFA regexToDfa(RegularExpression regex) {
        return NfaToDfa.convert(RegexToNfa.convert(regex));
    }

    private static SortedSet<String> generateTestValues(DFA dfa1, DFA dfa2, int n, Random random) {
        final var positive1 = generateTestValues(dfa1, n, random);
        final var positive2 = generateTestValues(dfa2, n, random);
        final var negative1 = generateTestValues(dfa1.negate(), n, random);
        final var negative2 = generateTestValues(dfa2.negate(), n, random);
        return SSet.concat(
                SSet.concat(positive1, positive2),
                SSet.concat(negative1, negative2)
        );
    }

    private static SortedSet<String> generateTestValues(DFA dfa, int n, Random random) {
        return Stream
                .generate(() -> 0)
                .parallel()
                .limit(n * 2)
                .map(ignore -> DfaToRandomString.generate(dfa, random))
                .distinct()
                .limit(n)
                .collect(Collectors.toCollection(TreeSet::new));
    }
}