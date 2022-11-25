package scenarioCreator.generation.evaluation;

import scenarioCreator.data.Language;
import scenarioCreator.generation.heterogeneity.Distance;
import scenarioCreator.generation.processing.Scenario;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.GermaNetInterface;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.WordNetInterface;
import scenarioCreator.generation.processing.tree.DistanceDefinition;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public final class ReachableConfigurations {
    private ReachableConfigurations() {
    }

    public static Stream<Reachability> reachability(String path, int startIndex, int rounds) {
        final var possibleAverageList = Stream
                .iterate(0.15, avg -> avg + 0.15)
                .limit(16)
                .filter(avg -> avg < 0.91)
                .toList();

        try {
            final var germanet = new GermaNetInterface();
            final var ulc = new UnifiedLanguageCorpus(Map.of(Language.German, germanet, Language.English, new WordNetInterface()));

            return possibleAverageList.stream()
                    .flatMap(avgStructural -> possibleAverageList.stream()
                            .flatMap(avgLinguistic -> possibleAverageList.stream()
                                    .map(avgConstraintBased -> {
                                                final var target = new Distance(avgStructural, avgLinguistic, avgConstraintBased, Double.NaN);
                                                final var error = singleCaseMultipleSeeds(
                                                        ulc, path, startIndex, rounds, target
                                                );
                                                return new Reachability(target, error);
                                            }
                                    )
                            )
                    );
        } catch (XMLStreamException | IOException e) {
            throw new RuntimeException(e);
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
                new DistanceDefinition.Target(0.0, 0.0, 0.0)
        );
        final var config = new Evaluation.FullConfiguration(dd, 5, 64, 1);
        return Evaluation.runForester(config, ulc, path, seed, false);
    }

    public static DistanceDefinition.Target bufferize(double avg) {
        return new DistanceDefinition.Target(avg - 0.1, avg, avg + 0.1);
    }

    public record Reachability(Distance target, Distance error) {
    }
}
