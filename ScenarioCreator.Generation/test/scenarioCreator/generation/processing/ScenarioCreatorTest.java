package processing;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scenarioCreator.data.Context;
import scenarioCreator.data.Language;
import scenarioCreator.data.Schema;
import scenarioCreator.data.identification.IdSimple;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.generation.heterogeneity.Distance;
import scenarioCreator.generation.processing.ScenarioCreator;
import scenarioCreator.generation.processing.tree.DistanceDefinition;
import scenarioCreator.generation.processing.tree.IForester;
import scenarioCreator.generation.processing.tree.SchemaWithAdditionalData;
import scenarioCreator.generation.processing.tree.TreeGenerationDefinition;
import scenarioCreator.utils.SSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;


class ScenarioCreatorTest {

    @Execution(ExecutionMode.SAME_THREAD)
    @ParameterizedTest
    @ValueSource(ints = {3, 4})
    void create(int scenarioSize) {
        // --- Arrange
        ForesterCallLogger.resetLogging();
        final var validDefinition = new DistanceDefinition(
                new DistanceDefinition.Target(0.1, 0.3, 0.4),
                new DistanceDefinition.Target(0.1, 0.3, 0.4),
                new DistanceDefinition.Target(0.1, 0.3, 0.4),
                new DistanceDefinition.Target(0.1, 0.3, 0.4)
        );
        final var creator = new ScenarioCreator(validDefinition, ForesterCallLogger::new);
        final var startSchema = new Schema(
                new IdSimple(-1),
                new StringPlusNaked("name", Language.Technical),
                Context.getDefault(),
                SSet.of()
        );

        // --- Act
        final var scenario = creator.create(startSchema, scenarioSize, new Random());

        // -- Assert
        // we don't really care about the scenario
        Assertions.assertEquals(scenarioSize, scenario.size());
        final var targetValues = ForesterCallLogger.ctorCalls().stream()
                .map(params -> params.targetDefinition)
                .toList();
        final var stanniDistanceDefinition = new DistanceDefinition(
                        new DistanceDefinition.Target(0.1, Double.NaN, 0.4),
                        new DistanceDefinition.Target(0.1, Double.NaN, 0.4),
                        new DistanceDefinition.Target(0.1, Double.NaN, 0.4),
                        new DistanceDefinition.Target(0.1, Double.NaN, 0.4)
                );
        final var expectedTargetValues3 = List.of(
                stanniDistanceDefinition,
                stanniDistanceDefinition,
                new DistanceDefinition(
                        new DistanceDefinition.Target(0.35, Double.NaN, 0.35),
                        new DistanceDefinition.Target(0.35, Double.NaN, 0.35),
                        new DistanceDefinition.Target(0.35, Double.NaN, 0.35),
                        new DistanceDefinition.Target(0.35, Double.NaN, 0.35)
                )
        );
        final var expectedTargetValues4 = List.of(
                stanniDistanceDefinition,
                stanniDistanceDefinition,
                new DistanceDefinition(
                        new DistanceDefinition.Target(0.2, Double.NaN, 0.4),
                        new DistanceDefinition.Target(0.2, Double.NaN, 0.4),
                        new DistanceDefinition.Target(0.2, Double.NaN, 0.4),
                        new DistanceDefinition.Target(0.2, Double.NaN, 0.4)
                ),
                new DistanceDefinition(
                        new DistanceDefinition.Target(0.4, Double.NaN, 0.4),
                        new DistanceDefinition.Target(0.4, Double.NaN, 0.4),
                        new DistanceDefinition.Target(0.4, Double.NaN, 0.4),
                        new DistanceDefinition.Target(0.4, Double.NaN, 0.4)
                )
        );
        final var expectedTargetValues = (scenarioSize == 3) ? expectedTargetValues3 : expectedTargetValues4;
        Assertions.assertEquals(expectedTargetValues, targetValues);
    }

    class ForesterCallLogger implements IForester {
        private static final List<CtorCallArguments> _ctorCalls = new ArrayList<>();

        public ForesterCallLogger(
                DistanceDefinition validDefinition,
                DistanceDefinition targetDefinition
        ) {
            _ctorCalls.add(new CtorCallArguments(
                    validDefinition, targetDefinition
            ));
        }

        public static List<CtorCallArguments> ctorCalls() {
            return _ctorCalls;
        }

        /**
         * sets ctorCalls to []
         */
        public static void resetLogging() {
            _ctorCalls.clear();
        }

        @Override
        public SchemaWithAdditionalData createNext(
                SchemaWithAdditionalData rootSchema,
                TreeGenerationDefinition tgd,
                SortedSet<Schema> oldSchemaSet,
                Random random
        ) {
            final var newSchemaId = oldSchemaSet.size();
            final var newSchema = new Schema(
                    new IdSimple(newSchemaId),
                    new StringPlusNaked("hugo", Language.Technical),
                    Context.getDefault(),
                    SSet.of()
            );

            final var distanceList = oldSchemaSet.stream()
                    .map(s -> new Distance(0.2, 0.2, 0.2, 0.2))
                    .toList();
            return new SchemaWithAdditionalData(newSchema, distanceList);
        }

        record CtorCallArguments(
                DistanceDefinition validDefinition,
                DistanceDefinition targetDefinition
        ) {
        }
    }
}