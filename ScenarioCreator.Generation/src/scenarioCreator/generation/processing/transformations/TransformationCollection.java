package scenarioCreator.generation.processing.transformations;

import scenarioCreator.generation.processing.transformations.constraintBased.ForeignKeyRemover;
import scenarioCreator.generation.processing.transformations.constraintBased.FunctionalDependencyRemover;
import scenarioCreator.generation.processing.transformations.constraintBased.RegularCheckModifier;
import scenarioCreator.generation.processing.transformations.contextual.ChangeUnitOfMeasure;
import scenarioCreator.generation.processing.transformations.linguistic.*;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.Translation;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import scenarioCreator.generation.processing.transformations.structural.*;
import scenarioCreator.utils.SSet;

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.*;

public class TransformationCollection {

    private final SortedSet<ColumnTransformation> _allColumnTransformationSet;
    private final SortedSet<TableTransformation> _allTableTransformationSet;
    private final Map<TransformationParams, SortedSet<SchemaTransformation>> _allSchemaTransformationSet;
    private final boolean _shouldOnlyReturnOneTransformation;
    private final String _singleName;

    public TransformationCollection(
            UnifiedLanguageCorpus unifiedLanguageCorpus,
            Translation translation,
            String singleName
    ) {
        _allColumnTransformationSet = generateAllColumnTransformationSet(unifiedLanguageCorpus, translation);
        _allTableTransformationSet = generateAllTableTransformationSet(unifiedLanguageCorpus, translation);
        _allSchemaTransformationSet = generateAllSchemaTransformationSet(unifiedLanguageCorpus, translation);
        _shouldOnlyReturnOneTransformation = true;
        _singleName = singleName;
    }

    public TransformationCollection(
            UnifiedLanguageCorpus unifiedLanguageCorpus,
            Translation translation
    ) {
        _allColumnTransformationSet = generateAllColumnTransformationSet(unifiedLanguageCorpus, translation);
        _allTableTransformationSet = generateAllTableTransformationSet(unifiedLanguageCorpus, translation);
        _allSchemaTransformationSet = generateAllSchemaTransformationSet(unifiedLanguageCorpus, translation);
        _shouldOnlyReturnOneTransformation = false;
        _singleName = "";
    }

    private SortedSet<ColumnTransformation> generateAllColumnTransformationSet(
            UnifiedLanguageCorpus unifiedLanguageCorpus, Translation translation
    ) {
        return SSet.of(
                // constraintBased
                new RegularCheckModifier(),
                // contextual
                new ChangeUnitOfMeasure(),
                // linguistic
                new AddTypoToColumnName(),
                new ChangeLanguageOfColumnName(translation),
                new RenameColumn(unifiedLanguageCorpus),
                // structural
                new ChangeDataType(),
                new DeNullification(),
                new GroupColumnLeafsToNodeNested(),
                new UngroupColumnNodeToColumnLeafs()
        );
    }

    private SortedSet<TableTransformation> generateAllTableTransformationSet(
            UnifiedLanguageCorpus unifiedLanguageCorpus, Translation translation
    ) {
        return SSet.of(
                // constraintBased
                new FunctionalDependencyRemover(),
                // contextual
                // linguistic
                new AddTypoToTableName(),
                new ChangeLanguageOfTableName(translation),
                new RenameTable(unifiedLanguageCorpus),
                // structural
                new ColumnCollectionToTable(),
                new ColumnLeafsToTable(),
                new ColumnNodeToTable(),
                new GroupColumnLeafsToNode(),
                //new NullableToHorizontalInheritance(), // TODO(80:20): Es können erstmal keine TGDs dazu generiert werden
                new NullableToVerticalInheritance(),
                new RemoveColumn(),
                new RemoveTable()
        );
    }

    private Map<TransformationParams, SortedSet<SchemaTransformation>> generateAllSchemaTransformationSet(
            UnifiedLanguageCorpus unifiedLanguageCorpus,
            Translation translation
    ) {
        return Stream
                .iterate(0, x -> x + 1)
                .map(code -> new TransformationParams(
                        (code & 0b001) == 0,
                        (code & 0b010) == 0,
                        (code & 0b100) == 0)
                )
                .limit(0b1000)
                .collect(Collectors.toMap(
                        params -> params,
                        params -> generateAllSchemaTransformationSetInner(unifiedLanguageCorpus, translation, params)
                ));
    }

    private SortedSet<SchemaTransformation> generateAllSchemaTransformationSetInner(
            UnifiedLanguageCorpus unifiedLanguageCorpus,
            Translation translation,
            TransformationParams params
    ) {
        return SSet.of(
                // constraintBased
                new ForeignKeyRemover(),
                // contextual
                // linguistic
                new AddTypoToSchemaName(),
                new ChangeLanguageOfSchemaName(translation),
                new RenameSchema(unifiedLanguageCorpus),
                // structural
                new BinaryValueToTable(),
                new HorizontalInheritanceToNullable(0.5),
                new MergeColumns(params.keepForeignKeyIntegrity),
                new TableToColumnCollection(params.shouldConserveAllRecords),
                new TableToColumnLeafs(params.shouldStayNormalized, params.shouldConserveAllRecords),
                new TableToColumnNode(params.shouldStayNormalized, params.shouldConserveAllRecords)
        );
    }

    public SortedSet<Transformation> getAllTransformationSet(
            boolean keepForeignKeyIntegrity,
            boolean shouldConserveAllRecords,
            boolean shouldStayNormalized,
            boolean conservesFlatRelations
    ) {
        final var params = new TransformationParams(keepForeignKeyIntegrity, shouldConserveAllRecords, shouldStayNormalized);
        return Stream
                .concat(
                        _allColumnTransformationSet.stream(),
                        Stream.concat(
                                _allTableTransformationSet.stream(),
                                _allSchemaTransformationSet.get(params).stream()
                        )
                )
                .filter(t -> t.conservesFlatRelations() || !conservesFlatRelations)
                .filter(t -> !_shouldOnlyReturnOneTransformation || t.toString().split("@")[0].toLowerCase().equals(_singleName.toLowerCase()))
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private record TransformationParams(
            boolean keepForeignKeyIntegrity,
            boolean shouldConserveAllRecords,
            boolean shouldStayNormalized
    ) {
    }
}
