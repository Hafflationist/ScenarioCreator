package scenarioCreator.generation.evaluation;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.utils.Correspondence;
import scenarioCreator.data.Language;
import scenarioCreator.data.Schema;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.identification.IdMerge;
import scenarioCreator.data.identification.IdPart;
import scenarioCreator.data.identification.IdSimple;
import scenarioCreator.data.primitives.StringPlus;
import scenarioCreator.data.table.InstancesOfTable;
import scenarioCreator.generation.heterogeneity.Distance;
import scenarioCreator.generation.inout.SchemaFileHandler;
import scenarioCreator.generation.processing.Scenario;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.LinguisticUtils;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.GermaNetInterface;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.WordNetInterface;
import scenarioCreator.generation.processing.tree.DistanceDefinition;
import scenarioCreator.generation.processing.tree.SchemaAsResult;
import scenarioCreator.generation.processing.tree.TgdChainElement;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.StreamExtensions;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class KörnerkissenEvaluator {
    private KörnerkissenEvaluator() {
    }

    public static void printScenario(Path path, int startIndex, int numberOfSchemas, double hetStructural, double hetLinguistig) {
        try {
            final var germanet = new GermaNetInterface();
            final var ulc = new UnifiedLanguageCorpus(Map.of(Language.German, germanet, Language.English, new WordNetInterface()));

            final var target = new Distance(hetStructural, hetLinguistig, 0.1, Double.NaN);
            final var scenario = getRealScenario(
                    ulc, path, startIndex, target, numberOfSchemas
            );
            final List<InstancesOfTable> initialInstancesOfTableList = List.of(); // TODO: get instances out of input path
            save(path, scenario, initialInstancesOfTableList);
        } catch (XMLStreamException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Scenario getRealScenario(
            UnifiedLanguageCorpus ulc, Path path, int startIndex, Distance target, int numberOfSchemas
    ) {
        // Da die Kreation manchmal fehlschlägt, wird hier am laufenden Band neu berechnet bis es klappt.
        final var scenarioOpt = Stream
                .iterate(startIndex, seed -> seed + 1)
                .limit(100)
                .map(seed -> getScenarioOpt(ulc, path, seed, target, numberOfSchemas))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
        if (scenarioOpt.isEmpty()) {
            throw new RuntimeException("REEE: Kann kein Schema generieren! Manchmal hilft das umstellen des Samens.");
        }
        return scenarioOpt.get();
    }

    private static Optional<Scenario> getScenarioOpt(
            UnifiedLanguageCorpus ulc, Path path, int seed, Distance target, int numberOfSchemas
    ) {
        System.out.println("Körnerkissen.Evaluation.getScenarioOpt with seed = " + seed + " and target = " + target);
        final var dd = new DistanceDefinition(
                bufferize(target.structural()),
                bufferize(target.linguistic()),
                bufferize(target.constraintBased()),
                new DistanceDefinition.Target(0.0, Double.NaN, 1.0)
        );
        final var config = new Evaluation.FullConfiguration(dd, numberOfSchemas, 64, 1);
        return Evaluation.runForester(config, ulc, path.toString(), seed, false);
    }

    private static DistanceDefinition.Target bufferize(double avg) {
        return new DistanceDefinition.Target(avg - 0.1, avg, avg + 0.1);
    }


    //
    // CORRS
    // CORRS CORRS CORRS
    private static Stream<Id> flatten(Id id) {
        return switch (id) {
            case IdSimple ignore -> Stream.of(id);
            case IdMerge idm -> Stream.concat(flatten(idm.predecessorId1()), flatten(idm.predecessorId2()));
            case IdPart idp -> flatten(idp.predecessorId());
        };
    }

    private static boolean isCorresponding(Column left, Column right) {
        final var leftIdStream = flatten(left.id());
        final var rightIdList = flatten(right.id()).toList();
        return leftIdStream.anyMatch(rightIdList::contains);
    }

    private static KörnerkissenColumn columnToKörnerkissenColumn(Column col) {
        return new KörnerkissenColumn(col.name(), col.id());
    }

    private static List<Correspondence<KörnerkissenColumn>> getCorrs(Schema s1, Schema s2) {
        final var leftColumnStream = s1.tableSet().stream().flatMap(t -> t.columnList().stream());
        final var rightColumnList = s2.tableSet().stream().flatMap(t -> t.columnList().stream()).toList();
        return leftColumnStream.flatMap(leftColumn -> rightColumnList.stream()
                .filter(rightColumn -> isCorresponding(leftColumn, rightColumn))
                .map(rightColumn -> new Correspondence<>(
                                columnToKörnerkissenColumn(leftColumn),
                                columnToKörnerkissenColumn(rightColumn),
                                1.0
                        )
                )
        ).toList();
    }
    // CORRS CORRS CORRS
    // CORRS
    //

    private static String idx2String(int idx) {
        if (idx < 10) {
            return "0" + idx;
        } else {
            return Integer.toString(idx);
        }
    }

    private static Path stdPath(Path path, int idx, StringPlus name) {
        final var filename = idx2String(idx) + "-" + name.rawString(LinguisticUtils::merge) + ".yaml";
        return Path.of(path.toString(), filename);
    }

    private static void save(Path path, Scenario scenario, List<InstancesOfTable> initialInstancesOfTableList) throws IOException {
        final var sarIndexedList = StreamExtensions.zip(
                IntStream.iterate(1, i -> i + 1).boxed(),
                scenario.sarList().stream(),
                Pair::new
        ).toList();
        for (final var sarWithIndex : sarIndexedList) {
            final var idx = sarWithIndex.first();
            final var sar = sarWithIndex.second();
            final var newInstances = calculateInstances(sar.tgdChain(), initialInstancesOfTableList);
            for (final var newInstance : newInstances) {
                final var instancePath = stdPath(path, idx, newInstance.table().name());
                saveInstance(instancePath, newInstance);
            }
            final var schema = sar.schema();
            final var filePath = stdPath(path, idx, schema.name());
            SchemaFileHandler.save(schema, filePath);
        }
        final var sarPairList = sarIndexedList.stream()
                .flatMap(swi -> sarIndexedList.stream().map(swi2 -> new Pair<>(swi, swi2)))
                .filter(pair -> pair.first().first() < pair.second().first())
                .toList();
        for (final var sarPair : sarPairList) {
            final var corrList = getCorrs(sarPair.first().second().schema(), sarPair.second().second().schema());
            final var filename = idx2String(sarPair.first().first())
                    + "-"
                    + idx2String(sarPair.second().first())
                    + "-correspondences.yaml";
            final var filePath = Path.of(path.toString(), filename);
            final var mapper = new ObjectMapper(new YAMLFactory());
            mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
                    .withFieldVisibility(JsonAutoDetect.Visibility.PROTECTED_AND_PUBLIC));
            mapper.writeValue(filePath.toFile(), corrList);
        }
    }

    private static List<InstancesOfTable> calculateInstances(List<TgdChainElement> tgdChain, List<InstancesOfTable> initialInstancesOfTableList) {
        // TODO: implement me! (use Chateau)
        return List.of();
    }

    private static void saveInstance(Path filePath, InstancesOfTable iot) {
        // TODO: implement me!
    }

    public record KörnerkissenColumn(StringPlus name, Id id) {
    }
}
