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
import scenarioCreator.utils.Pair;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ReachableConfigurationsExtra {
    private static final String BASE_FOLDER = "./resultsOfEvaluation/reachabilityExtra/";

    private ReachableConfigurationsExtra() {
    }

    private static Stream<Reachability> reachability(String path, int startIndex, int rounds) {
        final var possibleMoeList = Stream
                .iterate(4, moe -> moe + 4)
                .limit(32)
                .filter(moe -> moe <= 64)
                .toList();

        final var possibleChildrenList = Stream
                .iterate(1, c -> c + 1)
                .limit(12)
                .filter(c -> c <= 4)
                .toList();

        try {
            final var germanet = new GermaNetInterface();
            final var ulc = new UnifiedLanguageCorpus(Map.of(Language.German, germanet, Language.English, new WordNetInterface()));

            final var configurationList = possibleChildrenList.stream()
                    .flatMap(children -> possibleMoeList.stream()
                            .map(moe -> new Pair<>(children, moe))
                    )
                    .filter(ReachableConfigurationsExtra::calculationMissing)
                    .toList();
            System.out.println("configuration list calculated!");
            return configurationList.stream()
//                    .limit(12)
//                    .parallel()
                    .map(pair -> {
                                final var children = pair.first();
                                final var maxExpansionSteps = pair.second();
                                final var reachability = singleCaseMultipleSeeds(
                                        ulc, path, startIndex, rounds, children, maxExpansionSteps
                                );
                                save(reachability);
                                return reachability;
                            }
                    );
        } catch (XMLStreamException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean calculationMissing(Pair<Integer, Integer> pair) {
        final var children = pair.first();
        final var maxExpansionSteps = pair.second();
        final var path = targetToPath(children, maxExpansionSteps, "config");
        final var file = new File(path.toUri());
        final var missing = !file.exists();
        if (missing) {
            return true;
        } else {
            final var out = "Configuration "
                    + String.format("%02d", children)
                    + "-"
                    + String.format("%02d", maxExpansionSteps)
                    + " already found! Skipping calculation.";
            System.out.println(out);
            return false;
        }
    }

    public static void printReachabilities(String path, int startIndex, int rounds) {
        final var reachabilityList = reachability(path, startIndex, rounds).toList();
        for (final var r : reachabilityList) {
            System.out.println(r.toString());
        }
    }

    private static Reachability singleCaseMultipleSeeds(
            UnifiedLanguageCorpus ulc, String path, int startIndex, int rounds, int children, int maxExpansionSteps
    ) {
        final var target = new Distance(0.5, 0.3, 0.1, Double.NaN);
        // Die Distanzliste stellt den Fehler über Anläufe dar.
        // TODO: An genau dieser Stelle könnte man auch die Varianz bestimmen!
        final var pairList = Stream
                .iterate(startIndex, seed -> seed + 1)
                .limit(rounds)
//                .parallel()
                .map(seed -> singleCase(ulc, path, seed, children, maxExpansionSteps))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(pair -> new Pair<>(rateScenario(pair.first(), target), pair.second()))
                .toList();
        final var distanceList = pairList.stream()
                .map(Pair::first)
                .toList();

        final var distance = Distance.avg(distanceList);
        final var milli = (long) pairList.stream()
                .mapToLong(Pair::second)
                .average()
                .orElse(-1);
        return new Reachability(children, maxExpansionSteps, distance, milli);
    }

    private static Distance rateScenario(Scenario scenario, Distance avg) {
        return Distance.diff(avg, scenario.avgDistance());
    }

    private static Optional<Pair<Scenario, Long>> singleCase(
            UnifiedLanguageCorpus ulc, String path, int seed, int children, int maxExpansionSteps
    ) {
        System.out.println("ReachableConfigurations.singleCase with seed = " + seed + " and children = " + children + " and maxExpansionSteps = " + maxExpansionSteps);
        final var dd = new DistanceDefinition(
                bufferize(0.5),
                bufferize(0.3),
                bufferize(0.1),
                new DistanceDefinition.Target(0.0, Double.NaN, 1.0)
        );
        final var config = new Evaluation.FullConfiguration(dd, 5, maxExpansionSteps, children);
        final var start = System.currentTimeMillis();
        final var scenarioOpt = Evaluation.runForester(config, ulc, path, seed, true);
        final var end = System.currentTimeMillis();
        final var diff = end - start;
        return scenarioOpt.map(scenario -> new Pair<>(scenario, diff));
    }

    private static DistanceDefinition.Target bufferize(double avg) {
        return new DistanceDefinition.Target(avg - 0.1, avg, avg + 0.1);
    }

    private static Path targetToPath(int children, int maxExpansionSteps, String prefix) {
        final var filename = prefix
                + String.format("%02d", children)
                + "-"
                + String.format("%02d", maxExpansionSteps)
                + ".yaml";
        return Path.of(BASE_FOLDER + filename);
    }

    private static String str(double x, int n) {
        final var dec = Math.pow(10.0, n);
        return new DecimalFormat("0.000").format(Math.round(x * dec) / dec);
    }

    private static void save(Reachability reachability) {
        final var path = targetToPath(reachability.children, reachability.maxExpansionSteps, "config");
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
        return new ReachabilityAggregated(r.children, r.maxExpansionSteps, aggrError, r.millis);
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
                .map(ReachableConfigurationsExtra::aggregate)
                .map(ra -> "children: " + ra.children
                        + " | maxExpansionSteps: " + String.format("%02d", ra.maxExpansionSteps)
                        + " | fehler: " + str(ra.error, 3)
                        + " | laufzeit: " + ra.millis
                        + "\n")
                .sorted()
                .collect(Collectors.joining());

        final var file = new File(BASE_FOLDER + "aggrConfig.txt");
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
    }


    public record Reachability(int children, int maxExpansionSteps, Distance error, long millis) {
    }

    public record ReachabilityAggregated(int children, int maxExpansionSteps, double error, long millis) {
    }
}