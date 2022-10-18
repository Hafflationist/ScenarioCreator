package de.mrobohm;

import de.mrobohm.data.Language;
import de.mrobohm.data.primitives.StringPlus;
import de.mrobohm.data.primitives.StringPlusNaked;
import de.mrobohm.data.primitives.synset.GermanSynset;
import de.mrobohm.heterogenity.StringDistances;
import de.mrobohm.heterogenity.structural.ted.Ted;
import de.mrobohm.inout.SchemaFileHandler;
import de.mrobohm.processing.preprocessing.SemanticSaturation;
import de.mrobohm.processing.transformations.SingleTransformationExecuter;
import de.mrobohm.processing.transformations.TransformationCollection;
import de.mrobohm.processing.transformations.linguistic.helpers.Translation;
import de.mrobohm.processing.transformations.linguistic.helpers.biglingo.GermaNetInterface;
import de.mrobohm.processing.transformations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import de.mrobohm.processing.transformations.linguistic.helpers.biglingo.WordNetInterface;
import de.mrobohm.processing.tree.*;
import de.mrobohm.utils.Pair;
import de.mrobohm.utils.SSet;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.Nullable;

import javax.xml.stream.XMLStreamException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {

    private static void writeRandomSchema(String path) {
        final var random = new Random();
        try {
            final var germanet = new GermaNetInterface();
            final var ulc = new UnifiedLanguageCorpus(Map.of(Language.German, germanet, Language.English, new WordNetInterface()));
            final var saturator = new SemanticSaturation(ulc);
            final var schemaNaked = RandomSchemaGenerator.generateRandomSchema(
                    random, 8, 8, germanet::pickRandomEnglishWord
            );
            final var schema = saturator.saturateSemantically(schemaNaked);
            SchemaFileHandler.save(schema, Path.of(path, "schema.yaml"));
            System.out.println("Working Directory = " + System.getProperty("user.dir"));
            System.out.println("Saved schema in \"" + path + "\"");
        } catch (XMLStreamException | IOException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void testGermaNetInterface() {
        try {
            final var germaNetInterface = new GermaNetInterface();

            final var transWord = "Hund";
            final var synsetIdSet = germaNetInterface
                    .estimateSynset(transWord, SSet.of())
                    .stream().map(x -> Integer.toString(((GermanSynset) x).id()))
                    .collect(Collectors.toCollection(TreeSet::new));
            final var pwnIds = new HashSet<>(germaNetInterface.word2EnglishSynset(germaNetInterface.estimateSynset(transWord, SSet.of())));
            System.out.println("synsetIdSet von " + transWord + ": (" + synsetIdSet.size() + ") " + String.join("; ", synsetIdSet));
            System.out.println("PwnIds von " + transWord + ": (" + pwnIds.size() + ") " + String.join("; ", pwnIds.stream().map(Record::toString).toList()));

            //            final var synsetIdSet1 = germaNetInterface
//                    .estimateSynset("Bank", SSet.of("Institut", "Datum", "Id", "hfuzd89we"));
//            final var synonymes1 = germaNetInterface.getSynonymes(synsetIdSet1);
//
//            final var synsetIdSet2 = germaNetInterface
//                    .estimateSynset("Bank", SSet.of("Stuhl", "Datum", "Id", "hfuzd89we"));
//            final var synonymes2 = germaNetInterface.getSynonymes(synsetIdSet2);
//
//            final var synsetIdSet3 = germaNetInterface
//                    .estimateSynset("Bank", SSet.of("Datum", "Id", "hfuzd89we"));
//            final var synonymes3 = germaNetInterface.getSynonymes(synsetIdSet3);
//
//            System.out.println("possible synonyms for BANK(\"Institut\", \"Datum\", \"Id\", \"hfuzd89we\"):");
//            System.out.println(synsetIdSet1.size());
//            for (String s : synonymes1) {
//                System.out.println(s);
//            }
//            System.out.println("--------------------------------------");
//            System.out.println("possible synonyms for BANK(\"Stuhl\", \"Datum\", \"Id\", \"hfuzd89we\"):");
//            System.out.println(synsetIdSet2.size());
//            for (String s : synonymes2) {
//                System.out.println(s);
//            }
//            System.out.println("--------------------------------------");
//            System.out.println("this synonyms for BANK(\"Datum\", \"Id\", \"hfuzd89we\"):");
//            System.out.println(synsetIdSet3.size());
//            for (String s : synonymes3) {
//                System.out.println(s);
//            }
//            System.out.println("--------------------------------------");
//
//            final var synsGerman = germaNetInterface.getSynonymes("Bank");
//            System.out.println("All synonyms for BANK:");
//            for (String s : synsGerman) {
//                System.out.println(s);
//            }
        } catch (IOException | XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }

    private static void testWordNetInterface() {
        try {
            final var wordNetInterface = new WordNetInterface();
            final var synsetIdSet1 = wordNetInterface
                    .estimateSynset("dog", SSet.of("food", "water", "Id", "hfuzd89we"));
            final var synonymes1 = wordNetInterface.getSynonymes(synsetIdSet1);

            final var synsetIdSet2 = wordNetInterface
                    .estimateSynset("dog", SSet.of("animal", "cat", "Id", "hfuzd89we"));
            final var synonymes2 = wordNetInterface.getSynonymes(synsetIdSet2);

            final var synsetIdSet3 = wordNetInterface
                    .estimateSynset("dog", SSet.of("Datum", "Id", "hfuzd89we"));
            final var synonymes3 = wordNetInterface.getSynonymes(synsetIdSet3);

            System.out.println("possible synonyms for DOG(\"food\", \"water\", \"Id\", \"hfuzd89we\"):");
            System.out.println(synsetIdSet1.size());
            for (String s : synonymes1) {
                System.out.println(s);
            }
            System.out.println("--------------------------------------");
            System.out.println("possible synonyms for DOG(\"animal\", \"cat\", \"Id\", \"hfuzd89we\"):");
            System.out.println(synsetIdSet2.size());
            for (String s : synonymes2) {
                System.out.println(s);
            }
            System.out.println("--------------------------------------");
            System.out.println("this synonyms for DOG(\"Datum\", \"Id\", \"hfuzd89we\"):");
            System.out.println(synsetIdSet3.size());
            for (String s : synonymes3) {
                System.out.println(s);
            }
            System.out.println("--------------------------------------");

            final var synsGerman = wordNetInterface.getSynonymes("dog");
            System.out.println("All synonyms for DOG:");
            for (String s : synsGerman) {
                System.out.println(s);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void equality() {
        final var list1 = SSet.of(1, 2, 3, 4);
        final var list2 = Stream.of(2, 3, 4, 1).collect(Collectors.toCollection(TreeSet::new));

        final var tr1 = new TestRecord(0, list1);
        final var tr2 = new TestRecord(1, list1);
        final var tr3 = new TestRecord(1, list2);

        final var cr1 = new ContainerRecord(1, SSet.of(tr1, tr2));
        final var cr2 = new ContainerRecord(1, SSet.of(tr1, tr2));


        System.out.println("tr1 == tr2: " + (cr1 == cr2));
        System.out.println("tr1.equals(tr2): " + cr1.equals(cr2));
    }

    private static void testUnifiedLanguageCorpus() {
        try {
            final var germaNetInterface = new GermaNetInterface();
            final var wordNetInterface = new WordNetInterface();
            final var ulc = new UnifiedLanguageCorpus(
                    Map.of(Language.German, germaNetInterface, Language.English, wordNetInterface)
            );
            final var w1 = new StringPlusNaked("Hund", Language.German);
            final var w2 = new StringPlusNaked("Katze", Language.German);
            final var w3 = new StringPlusNaked("Bank", Language.German);
            final var w4 = new StringPlusNaked("Dog", Language.English);
            final var w5 = new StringPlusNaked("Unsinnnnnnnnnn", Language.English);
            semanticDiffMaxxing(List.of(w1, w2, w3, w4, w5), ulc);
        } catch (IOException | XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }

    private static void semanticDiffMaxxing(List<StringPlus> wordList, UnifiedLanguageCorpus ulc) {
        wordList.stream()
                .flatMap(w1 -> wordList.stream()
                                .map(w2 -> new Pair<>(
                                        Stream.of(w1.rawString(), w2.rawString()).sorted().toList(),
//                                StringDistances.levenshteinNorm(w1.rawString(), w2.rawString())
//                                ulc.semanticDiff(w1, w2)
                                        Math.min(ulc.semanticDiff(w1, w2), StringDistances.levenshteinNorm(w1.rawString(), w2.rawString()))
                                ))
                )
                .distinct()
                .forEach(System.out::println);
    }

    private static void testTranslation() {
        try {
            final var germaNetInterface = new GermaNetInterface();
            final var wordNetInterface = new WordNetInterface();
            final var ulc = new UnifiedLanguageCorpus(
                    Map.of(Language.German, germaNetInterface, Language.English, wordNetInterface)
            );
            final var translation = new Translation(ulc);
            final var spn = new StringPlusNaked("highway-to-hell", Language.Technical);
            final var semanticSaturation = new SemanticSaturation(ulc);
            final var sps = semanticSaturation.saturateSemantically(spn, SSet.of());
            final var random = new Random();
            System.out.println("Eingabe: " + sps.rawString());
            System.out.println("Übersetzungen:");
            Stream
                    .generate(() -> translation.translate(sps, random))
                    .limit(10000)
                    .distinct()
                    .filter(Optional::isPresent)
                    .limit(10)
                    .map(Optional::get)
                    .forEach(newSps -> System.out.println("newSps: " + newSps.rawString()));
        } catch (IOException | XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }

    private static void testForesterInner(String pathStr, @Nullable Integer seedOpt, UnifiedLanguageCorpus ulc, GermaNetInterface gni) {
        try {
            // clean directory
            FileUtils.cleanDirectory(Path.of(pathStr, "scenario").toFile());

            // Manage randomness and seed
            final var metaRandom = new Random();
            final var seed = (seedOpt == null) ? metaRandom.nextInt() : seedOpt;
            System.out.println("Seed: " + seed);
            final var random = new Random(seed);
            final var seedFile = Path.of(pathStr, "scenario/seed.txt").toFile();
            try (final var writer = new FileWriter(seedFile)) {
                writer.write(Integer.toString(seed));
            }

            // calc
            final var ss = new SemanticSaturation(ulc);
            final var schemaNaked = RandomSchemaGenerator.generateRandomSchema(
                    random, 3, 3, gni::pickRandomEnglishWord
            );
            final var schema = ss.saturateSemantically(schemaNaked);
            final var swad = new SchemaWithAdditionalData(schema, List.of());
            final var translation = new Translation(ulc);
            final var forester = new Forester(
                    new SingleTransformationExecuter(ss),
                    new TransformationCollection(ulc, translation),
                    DistanceMeasures.getDefault(),
                    DistanceDefinition.getDefault(),
                    DistanceDefinition.getDefault()
            );
            final var tgd = new TreeGenerationDefinition(
                    true, true, true, false
            );
//            final var schemaList = forester.createNext(swad, tgd, SSet.of(), random).stream().toList();
//            System.out.println("Working Directory = " + System.getProperty("user.dir"));
//            SchemaFileHandler.save(schema, Path.of(pathStr, "scenario/schemaRoot.yaml"));
//            IntStream.range(0, schemaList.size())
//                    .forEach(idx -> {
//                        final var newSchema = schemaList.get(idx);
//                        final var path = Path.of(pathStr, "scenario/schemaDerivative" + idx + ".yaml");
//                        try {
//                            SchemaFileHandler.save(newSchema, path);
//                        } catch (IOException e) {
//                            throw new RuntimeException(e);
//                        }
//                        System.out.println("Saved schema in \"" + path + "\"");
//                    });
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }


    private static void testForester(String path) throws XMLStreamException, IOException {
        final var germanet = new GermaNetInterface();
        final var ulc = new UnifiedLanguageCorpus(Map.of(Language.German, germanet, Language.English, new WordNetInterface()));
        for (int i = 1500; i < Integer.MAX_VALUE; i++) {
            System.out.println("Starte Anlauf " + i + "...");
            testForesterInner(path, i, ulc, germanet);
            System.out.println("Anlauf " + i + " vollständig");
        }
    }

    private static void testTreeEditDistance() {
        final var random = new Random();
        try {
            final var germanet = new GermaNetInterface();
            final var schema1 = RandomSchemaGenerator.generateRandomSchema(
                    random, 8, 8, germanet::pickRandomEnglishWord
            );
            final var schema2 = RandomSchemaGenerator.generateRandomSchema(
                    random, 8, 8, germanet::pickRandomEnglishWord
            );
            final var distAbs = Ted.calculateDistanceAbsolute(schema1, schema2);
            final var distRel = Ted.calculateDistanceRelative(schema1, schema2);
            System.out.println("distAbs: " + distAbs);
            System.out.println("distRel: " + distRel);
        } catch (XMLStreamException | IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void main(String[] args) throws XMLStreamException, IOException {
        final var path = args[0];
//        writeRandomSchema(path);
//        testGermaNetInterface();
//        testWordNetInterface();
//        testUnifiedLanguageCorpus();
//        testTranslation();
//        testForester(path);
        testTreeEditDistance();
    }

    record TestRecord(int id, SortedSet<Integer> things) {
    }

    record ContainerRecord(int id, SortedSet<TestRecord> testis) {
    }
}