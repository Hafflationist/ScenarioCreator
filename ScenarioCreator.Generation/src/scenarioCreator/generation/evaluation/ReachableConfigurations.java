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
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public final class ReachableConfigurations {
    private ReachableConfigurations() {
    }

    public static Stream<Reachability> reachability(String path, int startIndex, int rounds) {
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

    public static boolean calculationMissing(Distance target)  {
        final var path = targetToPath(target);
        final var file = new File(path.toUri());
        final var missins = !file.exists();
        if (missins) {
            return true;
        }else {
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

    public static Distance singleCaseMultipleSeeds(
            UnifiedLanguageCorpus ulc, String path, int startIndex, int rounds, Distance target
    ) {
        // Die Distanzliste stellt den Fehler über Anläufe dar.
        // TODO: An genau dieser Stelle könnte man auch die Varianz bestimmen!
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

    public static Optional<Scenario> singleCase(
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
        return Evaluation.runForester(config, ulc, path, seed, false);
    }

    public static DistanceDefinition.Target bufferize(double avg) {
        return new DistanceDefinition.Target(avg - 0.1, avg, avg + 0.1);
    }

    public record Reachability(Distance target, Distance error) {
    }

    public static Path targetToPath(Distance target) {
        final var filename = "target"
                + Double.toString(Math.round(target.structural() * 10.0) / 10.0).substring(0, 3)
                + "-"
                + Double.toString(Math.round(target.linguistic() * 10.0) / 10.0).substring(0, 3)
                + "-"
                + Double.toString(Math.round(target.constraintBased() * 10.0) / 10.0).substring(0, 3)
                + "-"
                + Double.toString(target.contextual()).substring(0, 3)
                + ".yaml";
        return Path.of("./resultsOfEvaluation/reachability/" + filename);
    }

    public static void save(Reachability reachability) {
        final var path = targetToPath(reachability.target);
        final var mapper = new ObjectMapper(new YAMLFactory());
        try {
            mapper.writeValue(path.toFile(), reachability);
        } catch (IOException e) {
            System.out.println("Could not save file " + path + "!");
            System.out.println("Content: " + reachability);
            throw new RuntimeException(e);
        }
    }
}
