package scenarioCreator.generation.evaluation;

import atom.ProvenanceInformation;
import atom.RelationalAtom;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.lang3.NotImplementedException;
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
import scenarioCreator.data.table.Table;
import scenarioCreator.data.tgds.TupleGeneratingDependency;
import scenarioCreator.generation.heterogeneity.Distance;
import scenarioCreator.generation.inout.SchemaFileHandler;
import scenarioCreator.generation.processing.Scenario;
import scenarioCreator.generation.processing.integrity.IntegrityChecker;
import scenarioCreator.generation.processing.preprocessing.SemanticSaturation;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.LinguisticUtils;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.GermaNetInterface;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.WordNetInterface;
import scenarioCreator.generation.processing.tree.DistanceDefinition;
import scenarioCreator.generation.processing.tree.TgdChainElement;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.StreamExtensions;
import term.VariableType;

import javax.xml.stream.XMLStreamException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class KörnerkissenEvaluator {
    private KörnerkissenEvaluator() {
    }

    public static void printScenario(
            Pair<Schema, List<InstancesOfTable>> anfangsschemaUndInstanzen, Path path,
            int startIndex,
            int numberOfSchemas,
            double hetStructural, double hetLinguistig
    ) {
        try {
            final var germanet = new GermaNetInterface();
            final var ulc = new UnifiedLanguageCorpus(Map.of(Language.German, germanet, Language.English, new WordNetInterface()));
            // Anreichern des Startschemas:
            IntegrityChecker.assertValidSchema(anfangsschemaUndInstanzen.first());
            final var ss = new SemanticSaturation(ulc);
            final var semanticInitSchema = ss.saturateSemantically(anfangsschemaUndInstanzen.first());
            IntegrityChecker.assertValidSchema(semanticInitSchema);

            final var target = new Distance(hetStructural, hetLinguistig, 0.1, Double.NaN);
            final var scenario = getRealScenario(
                    semanticInitSchema, ulc, path, startIndex, target, numberOfSchemas
            );
            save(path, scenario, anfangsschemaUndInstanzen.second());
        } catch (XMLStreamException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Scenario getRealScenario(
            Schema anfangsschema,
            UnifiedLanguageCorpus ulc, Path path, int startIndex, Distance target, int numberOfSchemas
    ) {
        // Da die Kreation manchmal fehlschlägt, wird hier am laufenden Band neu berechnet bis es klappt.
        final var scenarioOpt = Stream
                .iterate(startIndex, seed -> seed + 1)
                .limit(100)
                .map(seed -> getScenarioOpt(anfangsschema, ulc, path, seed, target, numberOfSchemas))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
        if (scenarioOpt.isEmpty()) {
            throw new RuntimeException("REEE: Kann kein Schema generieren! Manchmal hilft das umstellen des Samens.");
        }
        return scenarioOpt.get();
    }

    private static Optional<Scenario> getScenarioOpt(
            Schema anfangsschema,
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
        return Evaluation.runForester(anfangsschema, config, ulc, path.toString(), seed, false);
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

    private static Path stdPath(Path path, int idx, StringPlus name, String suffix) {
        final var filename = idx2String(idx) + "-" + name.rawString(LinguisticUtils::merge) + "." + suffix;
        return Path.of(path.toString(), filename);
    }

    private static void save(Path path, Scenario scenario, List<InstancesOfTable> initialInstancesOfTableList) throws IOException {
        final var sarIndexedList = StreamExtensions.zip(
                IntStream.iterate(1, i -> i + 1).boxed(),
                scenario.sarList().stream(),
                Pair::new).toList();
        for (final var sarWithIndex : sarIndexedList) {
            final var idx = sarWithIndex.first();
            final var sar = sarWithIndex.second();
            final var newInstances = calculateInstances(sar.tgdChain(), initialInstancesOfTableList);
            for (final var newInstance : newInstances) {
                final var instancePath = stdPath(path, idx, newInstance.table().name(), "csv");
                saveInstance(instancePath, newInstance);
            }
            final var schema = sar.schema();
            final var filePath = stdPath(path, idx, schema.name(), "yaml");
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

    private static List<InstancesOfTable> calculateInstances(List<TgdChainElement> tgdChain, List<InstancesOfTable> instancesOfTableList) {
        if (tgdChain.isEmpty()) return instancesOfTableList;
        final var firstTgdChainElement = tgdChain.get(0);
        final var tailTgdChain = tgdChain.stream().skip(1).toList();
        final var nextInstancesOfTableList = chaseOneStep(firstTgdChainElement, instancesOfTableList);
        return calculateInstances(tailTgdChain, nextInstancesOfTableList);
    }

    private static List<InstancesOfTable> chaseOneStep(TgdChainElement tgdChainElement, List<InstancesOfTable> instancesOfTableList) {
        // Falls es keinen Vorgänger gibt, befinden wir uns beim Anfangsschema
        if (tgdChainElement.predecessor() == null) return instancesOfTableList;
        final var chateauSchema = getChateauSchema(tgdChainElement.predecessor(), tgdChainElement.schema());
        final var chateauAtoms = new LinkedHashSet<>(instancesOfTableList.stream()
                .flatMap(KörnerkissenEvaluator::getChateauInstance)
                .toList());
        final var chateauInstance = new instance.Instance(chateauAtoms, chateauSchema, instance.OriginTag.INSTANCE);
        final var chateauConstraintSet = new LinkedHashSet<>(tgdChainElement.tgdList().stream()
                .map(KörnerkissenEvaluator::getChateauConstraint)
                .toList());
        final var chateauInstanceNew = chase.Chase.chase(chateauInstance, chateauConstraintSet);
        return reconvertInstances(tgdChainElement.schema(), chateauInstanceNew);
    }

    private static List<InstancesOfTable> reconvertInstances(Schema schema, instance.Instance chateauInstance) {
        final var raGrouping = chateauInstance.getRelationalAtoms().stream()
                .collect(Collectors.groupingBy(RelationalAtom::getName));
        return raGrouping.keySet().stream()
                .map(tableName -> schema.tableSet().stream()
                        .filter(t -> prependNeu(t.name()).equals(tableName))
                        .findFirst())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(foundTable -> {
                    final var tableName = prependNeu(foundTable.name());
                    final var entryList = raGrouping.get(tableName).stream()
                            .map(ra -> reconvertEntries(ra, foundTable.columnList()))
                            .toList();
                    return new InstancesOfTable(foundTable, entryList);
                })
                .toList();
    }

    private static Map<Column, String> reconvertEntries(atom.RelationalAtom ra, List<Column> columnList) {
        return ra.getTerms().stream()
                .filter(term -> term instanceof term.Constant)
                .map(term -> (term.Constant) term)
                .map(constant -> columnList.stream()
                        .filter(column ->
                                prependNeu(column.name()).equals(constant.getName())
                        )
                        .findFirst()
                        .map(foundColumn -> new Pair<>(foundColumn, constant.getValue().toString()))
                )
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toMap(Pair::first, Pair::second));
    }

    private static constraints.Constraint getChateauConstraint(TupleGeneratingDependency tgd) {
        if (tgd.constraints().isEmpty()) {
            // Das ist der einfachste Fall, weil man hier 0 auf Einschränkungen achten muss:
            final var head = new LinkedHashSet<>(tgd.forallRows().stream().map(rr -> {
                final var name = prependAlt(rr.name());
                final var termArray = new ArrayList<>(rr.columnList().stream()
                        .map(column -> {
                            final var columnName = prependAlt(column.name());
                            return (term.Term) new term.Variable(VariableType.FOR_ALL, columnName, 1);
                        })
                        .toList());
                return new atom.RelationalAtom(name, termArray, false, false, new ProvenanceInformation(""));
            }).toList());
            final var body = new LinkedHashSet<>(tgd.existRows().stream().map(rr -> {
                final var name = prependAlt(rr.name());
                final var termArray = new ArrayList<>(rr.columnList().stream()
                        .map(column -> {
                            final var columnName = prependNeu(column.name());
                            return (term.Term) new term.Variable(VariableType.EXISTS, columnName, 1);
                        })
                        .toList());
                return new atom.RelationalAtom(name, termArray, false, false, new ProvenanceInformation(""));
            }).toList());
            return new constraints.Tgd(head, body);
        }
        throw new NotImplementedException("implement me!"); // TODO: Hier sollten die TGDs erstellt werden. Ansonsten werden alle nicht-trivialen TGDs nicht funktionieren!
    }


    private static Stream<atom.RelationalAtom> getChateauInstance(InstancesOfTable instancesOfTable) {
        final var table = instancesOfTable.table();
        return instancesOfTable.entries().stream()
                .map(entry -> getChateauEntry(table, entry));
    }

    private static atom.RelationalAtom getChateauEntry(Table table, Map<Column, String> entry) {
        final var name = prependAlt(table.name());

        final var termArray = new ArrayList<>(entry.keySet().stream()
                .map(column -> {
                    final var columnName = prependAlt(column.name());
                    final var value = entry.get(column);
                    return (term.Term) term.Constant.fromString(columnName, value);
                })
                .toList());
        return new atom.RelationalAtom(name, termArray, false, false, new ProvenanceInformation(""));
    }

    private static HashMap<String, LinkedHashMap<String, String>> getChateauSchema(
            Schema oldSchema,
            Schema newSchema
    ) {
        IntegrityChecker.assertValidSchema(oldSchema);
        IntegrityChecker.assertValidSchema(newSchema);
        final var t2nStream = Stream.concat(
                oldSchema.tableSet().stream().map(t -> new Pair<>(t, prependAlt(t.name()))),
                newSchema.tableSet().stream().map(t -> new Pair<>(t, prependNeu(t.name())))
        ).toList();

        System.out.println("all tables:");
        for (final var table : t2nStream) {
            System.out.println(table);
        }

        return new HashMap<>(t2nStream.stream()
                .collect(Collectors.toMap(
                        Pair::second,
                        pair ->
                                new LinkedHashMap<>(pair.first().columnList().stream()
                                        .map(c -> {
                                                    return oldSchema.tableSet().stream().anyMatch(oldT -> prependAlt(oldT.name()).equals(pair.second()))
                                                            ? prependAlt(c.name())
                                                            : prependNeu(c.name());
                                                }
                                        )
                                        .collect(Collectors.toMap(
                                                x -> x,
                                                ignore -> "string"
                                        )))
                )));
    }

    private static String prependAlt(StringPlus name) {
        return "alt-" + name.rawString(LinguisticUtils::merge);
    }

    private static String prependNeu(StringPlus name) {
        return "neu-" + name.rawString(LinguisticUtils::merge);
    }

    private static void saveInstance(Path filePath, InstancesOfTable iot) {
        //creating header line:
        final var csvHeader = "\"" + iot.table().columnList().stream()
                .map(column -> column.name().rawString(LinguisticUtils::merge))
                .collect(Collectors.joining("\",\"")) + "\"";
        final var csvValues = iot.entries().stream()
                .map(entry -> "\"" + iot.table().columnList().stream()
                        .map(entry::get)
                        .map(value -> value == null ? "" : value)
                        .collect(Collectors.joining("\",\"")) + "\"")
                .collect(Collectors.joining("\n"));
        final var csvString = csvHeader + "\n" + csvValues;
        try {
            System.out.println("Writing file " + filePath + "...");
            FileWriter f2 = new FileWriter(filePath.toFile(), false);
            f2.write(csvString);
            f2.close();
            System.out.println(".... written!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public record KörnerkissenColumn(StringPlus name, Id id) {
    }
}
