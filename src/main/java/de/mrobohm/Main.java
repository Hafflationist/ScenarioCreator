package de.mrobohm;

import de.mrobohm.inout.SchemaFileHandler;
import de.mrobohm.operations.linguistic.helpers.biglingo.GermaNetInterface;
import de.mrobohm.operations.linguistic.helpers.biglingo.WordNetInterface;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.util.Random;
import java.util.Set;

public class Main {

    private static void WriteRandomSchema(String path) {
        var random = new Random();
        var schema = RandomSchemaGenerator.generateRandomSchema(random, 8, 8);
        try {
            SchemaFileHandler.save(schema, path);
            System.out.println("Saved schema in \"" + path + "\"");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

    }

    private static void TestGermaNetInterface() {
        try {
            var germaNetInterface = new GermaNetInterface();
            var synsetIdSet1 = germaNetInterface
                    .estimateSynset("Bank", Set.of("Institut", "Datum", "Id", "hfuzd89we"));
            var synonymes1 = germaNetInterface.getSynonymes(synsetIdSet1);

            var synsetIdSet2 = germaNetInterface
                    .estimateSynset("Bank", Set.of("Stuhl", "Datum", "Id", "hfuzd89we"));
            var synonymes2 = germaNetInterface.getSynonymes(synsetIdSet2);

            var synsetIdSet3 = germaNetInterface
                    .estimateSynset("Bank", Set.of("Datum", "Id", "hfuzd89we"));
            var synonymes3 = germaNetInterface.getSynonymes(synsetIdSet3);

            System.out.println("possible synonyms for BANK(\"Institut\", \"Datum\", \"Id\", \"hfuzd89we\"):");
            System.out.println(synsetIdSet1.size());
            for (String s : synonymes1) {
                System.out.println(s);
            }
            System.out.println("--------------------------------------");
            System.out.println("possible synonyms for BANK(\"Stuhl\", \"Datum\", \"Id\", \"hfuzd89we\"):");
            System.out.println(synsetIdSet2.size());
            for (String s : synonymes2) {
                System.out.println(s);
            }
            System.out.println("--------------------------------------");
            System.out.println("this synonyms for BANK(\"Datum\", \"Id\", \"hfuzd89we\"):");
            System.out.println(synsetIdSet3.size());
            for (String s : synonymes3) {
                System.out.println(s);
            }
            System.out.println("--------------------------------------");

            var synsGerman = germaNetInterface.getSynonymes("Bank");
            System.out.println("All synonyms for BANK:");
            for (String s : synsGerman) {
                System.out.println(s);
            }
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


    public static void main(String[] args) {
        var path = args[0];
        WriteRandomSchema(path);
//        TestGermaNetInterface();
//        TestWordNetInterface();
    }
}