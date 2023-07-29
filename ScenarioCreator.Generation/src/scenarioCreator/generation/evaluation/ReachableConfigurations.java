package scenarioCreator.generation.evaluation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import scenarioCreator.data.Language;
import scenarioCreator.generation.heterogeneity.Distance;
import scenarioCreator.generation.processing.Scenario;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.GermaNetInterface;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.WordNetInterface;
import scenarioCreator.generation.processing.tree.DistanceDefinition;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ReachableConfigurations {
    private static final String BASE_FOLDER = "./resultsOfEvaluation/reachability/";

    private ReachableConfigurations() {
    }

    private static Stream<Reachability> reachability(String path, int startIndex, int rounds) {
        final var possibleAverageList = Stream
                .iterate(0.1, avg -> avg + 0.2)
                .limit(16)
                .filter(avg -> avg < 0.91)
                .toList();

        try {
            final var germanet = new GermaNetInterface();
            final var ulc = new UnifiedLanguageCorpus(Map.of(Language.German, germanet, Language.English, new WordNetInterface()));

            final var configurationList = possibleAverageList.stream()
                    .flatMap(avgStructural -> possibleAverageList.stream()
                            .flatMap(avgLinguistic -> possibleAverageList.stream()
                                    .map(avgConstraintBased -> new Distance(avgStructural, avgLinguistic, avgConstraintBased, Double.NaN))
                                    .filter(ReachableConfigurations::calculationMissing)
                            )
                    ).toList();
            System.out.println("configuration list calculated!");
            return configurationList.stream()
//                    .parallel()
                    .map(target -> {
                                final var error = singleCaseMultipleSeeds(
                                        ulc, path, startIndex, rounds, target
                                );
                                final var reachability = new Reachability(target, error);
                                save(reachability);
                                return reachability;
                            }
                    );
        } catch (XMLStreamException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean calculationMissing(Distance target) {
        final var path = targetToPath(target, "target");
        final var file = new File(path.toUri());
        final var missing = !file.exists();
        if (missing) {
            return true;
        } else {
            System.out.println("Configuration " + target + " already found! Skipping calculation.");
            return false;
        }
    }

    public static void printReachabilities(String path, int startIndex, int rounds) {
        final var reachabilityList = reachability(path, startIndex, rounds).toList();
        for (final var r : reachabilityList) {
            System.out.println(r.toString());
        }
    }

    private static Distance singleCaseMultipleSeeds(
            UnifiedLanguageCorpus ulc, String path, int startIndex, int rounds, Distance target
    ) {
        // Die Distanzliste stellt den Fehler über Anläufe dar.
        final var distanceList = Stream
                .iterate(startIndex, seed -> seed + 1)
                .limit(rounds)
//                .parallel()
                .map(seed -> singleCase(ulc, path, seed, target))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(s -> rateScenario(s, target))
                .toList();
        return Distance.avg(distanceList);
    }

    private static Distance rateScenario(Scenario scenario, Distance avg) {
        return Distance.diff(avg, scenario.avgDistance());
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

    private static void save(Reachability reachability) {
        final var path = targetToPath(reachability.target, "target");
        final var mapper = new ObjectMapper(new YAMLFactory());
        try {
            mapper.writeValue(path.toFile(), reachability);
        } catch (IOException e) {
            System.out.println("Could not save file " + path + "!");
            System.out.println("Content: " + reachability);
            throw new RuntimeException(e);
        }
    }

    private static ReachabilityAggregated aggregate(Reachability r) {
        var aggrError = Math.sqrt(Math.pow(r.error.structural(), 2.0) + Math.pow(r.error.linguistic(), 2.0) + Math.pow(r.error.constraintBased(), 2.0));
        return new ReachabilityAggregated(r.target, aggrError);
    }

    public static void postprocessing() {
        writeAggregatedError(readResults());
    }

    private static Stream<Reachability> readResults() {
        final var mapper = new ObjectMapper(new YAMLFactory());
        File folder = new File(BASE_FOLDER);
        return Arrays.stream(Objects.requireNonNull(folder.listFiles()))
                .map(file -> {
                    try {
                        return Optional.of(mapper.readValue(file, Reachability.class));
                    } catch (IOException e) {
                        return Optional.<Reachability>empty();
                    }
                })
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    private static void writeAggregatedError(Stream<Reachability> reachabilityStream) {
        final var content = reachabilityStream
                .map(ReachableConfigurations::aggregate)
                .map(ra -> {
                    final var t = ra.target;
                    return str(t.structural(), 1)
                            + "-"
                            + str(t.linguistic(), 1)
                            + "-"
                            + str(t.constraintBased(), 1)
                            + " -> "
                            + str(ra.error, 3)
                            + "\n";
                })
                .sorted()
                .collect(Collectors.joining());

        final var file = new File(BASE_FOLDER + "aggrTarget.txt");
        if (file.exists()) return;

        try {
            if (file.createNewFile()) {
                try (final var writer = new FileWriter(file)) {
                    writer.write(content);
                }
            }
        } catch (IOException e) {
            System.err.println("Could not create file " + file);
        }
    }


    public record Reachability(Distance target, Distance error) {
    }

    public record ReachabilityAggregated(Distance target, double error) {
    }
}
