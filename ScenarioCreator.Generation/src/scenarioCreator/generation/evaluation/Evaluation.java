package scenarioCreator.generation.evaluation;

import org.apache.commons.io.FileUtils;
import scenarioCreator.data.Schema;
import scenarioCreator.generation.heterogeneity.constraintBased.CheckNumericalBasedDistanceMeasure;
import scenarioCreator.generation.heterogeneity.constraintBased.FunctionalDependencyBasedDistanceMeasure;
import scenarioCreator.generation.heterogeneity.linguistic.LinguisticDistanceMeasure;
import scenarioCreator.generation.heterogeneity.structural.ted.Ted;
import scenarioCreator.generation.processing.Scenario;
import scenarioCreator.generation.processing.ScenarioCreator;
import scenarioCreator.generation.processing.integrity.IdentificationNumberCalculator;
import scenarioCreator.generation.processing.preprocessing.SemanticSaturation;
import scenarioCreator.generation.processing.transformations.SingleTransformationExecutor;
import scenarioCreator.generation.processing.transformations.TransformationCollection;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.Translation;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import scenarioCreator.generation.processing.tree.DistanceDefinition;
import scenarioCreator.generation.processing.tree.DistanceMeasures;
import scenarioCreator.generation.processing.tree.Forester;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Evaluation {
    private Evaluation() {
    }

    public static void transformationCount(FullConfiguration config, UnifiedLanguageCorpus ulc, String path, int startIndex, int rounds) {
        final var allUsedTransformationList = Stream
                .iterate(startIndex, i -> i + 1)
                .limit(rounds)
//                .parallel()
                .map(i -> runForester(config, ulc, path, i, false))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .flatMap(scenario -> scenario.sarList().stream())
                .flatMap(sar -> sar.executedTransformationList().stream())
                .toList();
        final var countPerTransformation = allUsedTransformationList.stream()
                .distinct()
                .collect(Collectors.toMap(
                        Function.identity(),
                        transformationName -> allUsedTransformationList.stream()
                                .filter(t -> t.equals(transformationName))
                                .count()
                ));
        final var transformationSortedByCount = countPerTransformation.keySet().stream()
                .sorted(Comparator.comparing(countPerTransformation::get))
                .toList();
        System.out.println("Nach Häufigkeit");
        for (final var t : transformationSortedByCount) {
            System.out.println(t + "\t: " + countPerTransformation.get(t));
        }
        System.out.println("\nAlphabetisch");
        for (final var t : countPerTransformation.keySet().stream().sorted().toList()) {
            System.out.println(t + "\t: " + countPerTransformation.get(t));
        }
    }

    private static Optional<Scenario> runForesterInner(
            FullConfiguration config, String pathStr, Schema initSchema, int seed, UnifiedLanguageCorpus ulc, boolean debug
    ) {
        try {
            // clean directory
            System.out.println("Kurz vor erster Ausgabe ins Ausgabeverzeichnis...");
            final var path = Path.of(pathStr, "scenario");
            Files.createDirectories(path);
            FileUtils.cleanDirectory(path.toFile());

            System.out.println("Samen schreiben: " + seed);
            final var random = new Random(seed);
            final var seedFile = Path.of(pathStr, "scenario/seed.txt").toFile();
            try (final var writer = new FileWriter(seedFile)) {
                writer.write(Integer.toString(seed));
            }

            // calc
            final var ss = new SemanticSaturation(ulc);

            final var allIdList = IdentificationNumberCalculator.getAllIds(initSchema, false).toList();
            final var nonUniqueIdSet = allIdList.stream()
                    .filter(id -> allIdList.stream().filter(id2 -> Objects.equals(id2, id)).count() >= 2)
                    .collect(Collectors.toCollection(TreeSet::new));
            System.out.println("Prüfung des Anfangsschemas...");
            if (!nonUniqueIdSet.isEmpty()) {
                System.out.println("Anfangsschema war fehlerhaft! Starte neuen Versuch...");
                return Optional.empty(); // Generation of start schema broken... (skip and forget)
            }

            final var distanceMeasures = new DistanceMeasures(
                    (s1, s2) -> {
                        try {
                            return Ted.calculateDistanceRelative(s1, s2);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    },
                    (s1, s2) -> LinguisticDistanceMeasure.calculateDistanceToRootRelative(s1, s2, ulc::semanticDiff),
                    (s1, s2) -> {
                        final var numDist = CheckNumericalBasedDistanceMeasure.calculateDistanceRelative(s1, s2);
                        final var regDist = CheckNumericalBasedDistanceMeasure.calculateDistanceRelative(s1, s2);
                        final var funDist = FunctionalDependencyBasedDistanceMeasure.calculateDistanceRelative(s1, s2);
                        return (numDist + regDist + funDist) / 3.0;
                    },
                    (__, ___) -> 0.0
            );

            final var translation = new Translation(ulc);
            System.out.println("Kreator kreiert!!!");
            final var creator = new ScenarioCreator(config.dd, ((validDefinition, targetDefinition) -> new Forester(
                    new SingleTransformationExecutor(ss),
                    new TransformationCollection(ulc, translation),
                    distanceMeasures,
                    validDefinition,
                    targetDefinition,
                    config.treeSteps
            )));
//            System.out.println("Preparations finished (rnd: " + random.nextInt(1000) + ")");
//            System.out.println("Scenario created!");
            return Optional.of(creator.create(initSchema, config.scenarioSize, config.newChildren(), random, debug));
        } catch (IOException e) {
            System.err.println("REEEEE!! Du hast ein ungültiges (nicht existent oder nicht schreibbar) Verzeichnis angegeben. Folgend die Eingabe:");
            System.err.println(e.getMessage());
            return Optional.empty();
        }
    }


    public static Optional<Scenario> runForester(
            FullConfiguration config, UnifiedLanguageCorpus ulc, String path, int seed, boolean debug
    ) {
        System.out.println("Anfangsschema laden");
        final var initSchema = Init.getInitSchema(ulc);
        System.out.println("Anfangsschema geladen!");
        return runForesterInner(config, path, initSchema, seed, ulc, debug);
    }

    public record FullConfiguration(DistanceDefinition dd, int scenarioSize, int treeSteps, int newChildren) {
    }
}
