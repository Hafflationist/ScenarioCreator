package scenarioCreator.generation.evaluation;

import scenarioCreator.data.Language;
import scenarioCreator.generation.heterogeneity.Distance;
import scenarioCreator.generation.inout.SchemaFileHandler;
import scenarioCreator.generation.processing.Scenario;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.LinguisticUtils;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.GermaNetInterface;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.WordNetInterface;
import scenarioCreator.generation.processing.tree.DistanceDefinition;
import scenarioCreator.generation.processing.tree.SchemaAsResult;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.StreamExtensions;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class KörnerkissenEvaluator {
    private KörnerkissenEvaluator() {
    }

    public static void printScenario(Path path, int startIndex, double hetStructural, double hetLinguistig) {
        try {
            final var germanet = new GermaNetInterface();
            final var ulc = new UnifiedLanguageCorpus(Map.of(Language.German, germanet, Language.English, new WordNetInterface()));

            final var target = new Distance(hetStructural, hetLinguistig, 0.1, Double.NaN);
            final var scenario = getRealScenario(
                    ulc, path, startIndex, target
            );
            save(path, scenario);
        } catch (XMLStreamException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Scenario getRealScenario(
            UnifiedLanguageCorpus ulc, Path path, int startIndex, Distance target
    ) {
        // Da die Kreation manchmal fehlschlägt, wird hier am laufenden Band neu berechnet bis es klappt.
        final var scenarioOpt = Stream
                .iterate(startIndex, seed -> seed + 1)
                .limit(100)
                .map(seed -> getScenarioOpt(ulc, path, seed, target))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
        if (scenarioOpt.isEmpty()) {
            throw new RuntimeException("REEE: Kann kein Schema generieren! Manchmal hilft das umstellen des Samens.");
        }
        return scenarioOpt.get();
    }

    private static Optional<Scenario> getScenarioOpt(
            UnifiedLanguageCorpus ulc, Path path, int seed, Distance target
    ) {
        System.out.println("Körnerkissen.Evaluation.getScenarioOpt with seed = " + seed + " and target = " + target);
        final var dd = new DistanceDefinition(
                bufferize(target.structural()),
                bufferize(target.linguistic()),
                bufferize(target.constraintBased()),
                new DistanceDefinition.Target(0.0, Double.NaN, 1.0)
        );
        final var config = new Evaluation.FullConfiguration(dd, 2, 64, 1);
        return Evaluation.runForester(config, ulc, path.toString(), seed, false);
    }

    private static DistanceDefinition.Target bufferize(double avg) {
        return new DistanceDefinition.Target(avg - 0.1, avg, avg + 0.1);
    }

    private static void save(Path path, Scenario scenario) throws IOException {
        final var schemaIndexedList = StreamExtensions.zip(
                IntStream.iterate(1, i -> i + 1).boxed(),
                scenario.sarList().stream()
                        .map(SchemaAsResult::schema),
                Pair::new
        ).toList();
        for (final var schemaWithIndex : schemaIndexedList) {
            final var schema = schemaWithIndex.second();
            final var idx = schemaWithIndex.first();
            final var filename = idx + "-" + schema.name().rawString(LinguisticUtils::merge)+ ".yaml";
            final var filePath = Path.of(path.toString(), filename);
            SchemaFileHandler.save(schema, filePath);
        }
    }
}
