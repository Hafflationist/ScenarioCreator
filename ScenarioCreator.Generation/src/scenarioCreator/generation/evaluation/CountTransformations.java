package scenarioCreator.generation.evaluation;

import scenarioCreator.data.Language;
import scenarioCreator.generation.heterogeneity.Distance;
import scenarioCreator.generation.processing.Scenario;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.GermaNetInterface;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.WordNetInterface;
import scenarioCreator.generation.processing.tree.DistanceDefinition;
import scenarioCreator.utils.Pair;

import javax.xml.stream.XMLStreamException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class CountTransformations {
    private static final String BASE_FOLDER = "./resultsOfEvaluation/count/";

    private CountTransformations() {
    }

    private static Map<String, Long> count(List<Scenario> scenarioList) {
        final var transformationList = scenarioList.stream()
                .flatMap(s -> s.sarList().stream())
                .flatMap(sar -> sar.executedTransformationList().stream())
                .toList();
        return transformationList.stream()
                .distinct()
                .collect(Collectors.toMap(
                        Function.identity(),
                        t -> transformationList.stream().filter(tt -> tt.equals(t)).count()
                ));
    }

    private static Stream<Pair<Distance, List<Scenario>>> generateScenarios(String path, int seed, int rounds) {
        try {
            final var germanet = new GermaNetInterface();
            final var ulc = new UnifiedLanguageCorpus(Map.of(Language.German, germanet, Language.English, new WordNetInterface()));

            final var configurationList = List.of(
//                    new Distance(0.1, 0.1, 0.1, Double.NaN),
//                    new Distance(0.5, 0.1, 0.1, Double.NaN),
//                    new Distance(0.5, 0.3, 0.1, Double.NaN)
                    new Distance(0.1, 0.5, 0.1, Double.NaN)
            );
            return configurationList.stream()
                    .map(target -> new Pair<>(target, multipleCases(ulc, path, seed, rounds, target)));
        } catch (XMLStreamException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void printCount(String path, int seed, int rounds) {
        generateScenarios(path, seed, rounds)
                .forEach(pair -> {
                    final var resultPath = targetToPath(pair.first(), "target");
                    final var content = mapToString(count(pair.second()));
                    final var file = resultPath.toFile();
                    if (file.exists()) {
                        file.delete();
                    }
                    try {
                        if (file.createNewFile()) {
                            try (final var writer = new FileWriter(file)) {
                                writer.write(content);
                            }
                        }
                    } catch (IOException e) {
                        System.err.println("Could not create file " + file);
                    }
                });
    }

    private static String mapToString(Map<String, Long> map) {
        return map.entrySet().stream()
                .sorted(Comparator.comparingLong(e -> -e.getValue()))
                .map(e -> e.getKey() + " \t -> " + e.getValue())
                .collect(Collectors.joining("\n"));
    }

    private static List<Scenario> multipleCases(
            UnifiedLanguageCorpus ulc, String path, int seed, int rounds, Distance target
    ) {
        return Stream.iterate(seed, i -> i + 1)
                .limit(rounds)
                .map(s -> singleCase(ulc, path, s, target))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toList();
    }

    private static Optional<Scenario> singleCase(
            UnifiedLanguageCorpus ulc, String path, int seed, Distance target
    ) {
        System.out.println("ReachableConfigurations.singleCase with seed = " + seed + " and target = " + target);
        final var dd = new DistanceDefinition(
                bufferize(target.structural()),
                bufferize(target.linguistic()),
                bufferize(target.constraintBased()),
                new DistanceDefinition.Target(0.0, Double.NaN, 1.0)
        );
        final var config = new Evaluation.FullConfiguration(dd, 5, 64, 1);
        return Evaluation.runForester(Init.getInitSchema(ulc), config, ulc, path, seed, false);
    }

    private static DistanceDefinition.Target bufferize(double avg) {
        return new DistanceDefinition.Target(avg - 0.1, avg, avg + 0.1);
    }

    private static Path targetToPath(Distance target, String prefix) {
        final var filename = prefix
                + str(target.structural(), 1)
                + "-"
                + str(target.linguistic(), 1)
                + "-"
                + str(target.constraintBased(), 1)
                + "-"
                + Double.toString(target.contextual()).substring(0, 3)
                + ".yaml";
        return Path.of(BASE_FOLDER + filename);
    }

    private static String str(double x, int n) {
        final var dec = Math.pow(10.0, n);
        final var xStr = Double.toString(Math.round(x * dec) / dec);
        return xStr.substring(0, Math.min(2 + n, xStr.length()));
    }

}
