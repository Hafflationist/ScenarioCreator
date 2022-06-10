package de.mrobohm;

import de.mrobohm.data.Language;
import de.mrobohm.inout.SchemaFileHandler;
import de.mrobohm.operations.linguistic.helpers.biglingo.GermaNetInterface;
import de.mrobohm.operations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import de.mrobohm.operations.linguistic.helpers.biglingo.WordNetInterface;
import de.mrobohm.preprocessing.SemanticSaturation;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {

    private static void WriteRandomSchema(String path) {
        var random = new Random();
        try {
            var ulc = new UnifiedLanguageCorpus(Map.of(Language.German, new GermaNetInterface(), Language.English, new WordNetInterface()));
            var saturator = new SemanticSaturation(ulc);
            var schema = saturator.saturateSemantically(RandomSchemaGenerator.generateRandomSchema(random, 8, 8));
            SchemaFileHandler.save(schema, path);
            System.out.println("Saved schema in \"" + path + "\"");
        } catch (XMLStreamException | IOException e) {
            System.out.println(e.getMessage());
        }

    }

    private static void TestGermaNetInterface() {
        try {
            var germaNetInterface = new GermaNetInterface();

            var transWord = "gehen";
            var synsetIdSet = germaNetInterface
                    .estimateSynset(transWord, Set.of())
                    .stream().map(x -> Integer.toString(x))
                    .collect(Collectors.toSet());
            var pwnIds = new HashSet<>(germaNetInterface.word2InterLingoRecord(transWord, Set.of()));
            System.out.println("synsetIdSet von " + transWord + ": (" + synsetIdSet.size() + ") " + String.join("; ", synsetIdSet));
            System.out.println("PwnIds von " + transWord + ": (" + pwnIds.size() + ") " + String.join("; ", pwnIds.stream().map(Record::toString).toList()));
            var reTranslation = pwnIds.stream()
                    .flatMap(pwnId -> germaNetInterface.interLingoRecord2Word(pwnId).stream()).toList();
            System.out.println("Rückübersetzungen von " + transWord + ":" + String.join(". ", reTranslation));

            //            var synsetIdSet1 = germaNetInterface
//                    .estimateSynset("Bank", Set.of("Institut", "Datum", "Id", "hfuzd89we"));
//            var synonymes1 = germaNetInterface.getSynonymes(synsetIdSet1);
//
//            var synsetIdSet2 = germaNetInterface
//                    .estimateSynset("Bank", Set.of("Stuhl", "Datum", "Id", "hfuzd89we"));
//            var synonymes2 = germaNetInterface.getSynonymes(synsetIdSet2);
//
//            var synsetIdSet3 = germaNetInterface
//                    .estimateSynset("Bank", Set.of("Datum", "Id", "hfuzd89we"));
//            var synonymes3 = germaNetInterface.getSynonymes(synsetIdSet3);
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
//            var synsGerman = germaNetInterface.getSynonymes("Bank");
//            System.out.println("All synonyms for BANK:");
//            for (String s : synsGerman) {
//                System.out.println(s);
//            }
        } catch (IOException | XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }

    private static void TestWordNetInterface() {
        try {
            var wordNetInterface = new WordNetInterface();
            var synsetIdSet1 = wordNetInterface
                    .estimateSynset("dog", Set.of("food", "water", "Id", "hfuzd89we"));
            var synonymes1 = wordNetInterface.getSynonymes(synsetIdSet1);

            var synsetIdSet2 = wordNetInterface
                    .estimateSynset("dog", Set.of("animal", "cat", "Id", "hfuzd89we"));
            var synonymes2 = wordNetInterface.getSynonymes(synsetIdSet2);

            var synsetIdSet3 = wordNetInterface
                    .estimateSynset("dog", Set.of("Datum", "Id", "hfuzd89we"));
            var synonymes3 = wordNetInterface.getSynonymes(synsetIdSet3);

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

            var synsGerman = wordNetInterface.getSynonymes("dog");
            System.out.println("All synonyms for DOG:");
            for (String s : synsGerman) {
                System.out.println(s);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void Equality() {
        var list1 = Set.of(1, 2, 3, 4);
        var list2 = Stream.of(2, 3, 4, 1).collect(Collectors.toSet());

        var tr1 = new TestRecord(0, list1);
        var tr2 = new TestRecord(1, list1);
        var tr3 = new TestRecord(1, list2);

        var cr1 = new ContainerRecord(1, Set.of(tr1, tr2));
        var cr2 = new ContainerRecord(1, Set.of(tr1, tr2));


        System.out.println("tr1 == tr2: " + (cr1 == cr2));
        System.out.println("tr1.equals(tr2): " + cr1.equals(cr2));

    }


    public static void main(String[] args) {
        var path = args[0];
//        WriteRandomSchema(path);
//        TestGermaNetInterface();
//        TestWordNetInterface();
//        Equality();
    }

    record TestRecord(int id, Set<Integer> things) {
    }

    record ContainerRecord(int id, Set<TestRecord> testis) {
    }
}