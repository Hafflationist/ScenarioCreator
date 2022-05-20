package de.mrobohm;

import de.mrobohm.inout.SchemaFileHandler;
import de.mrobohm.operations.linguistic.helpers.biglingo.WordNetInterface;

import java.io.IOException;
import java.util.Random;

public class Main {

    private static void WriteRandomSchema(String path) {
        var random = new Random();
        var schema = RandomSchemaGenerator.generateRandomSchema(random);
        try {
            SchemaFileHandler.save(schema, path);
            System.out.println("Saved schema in \"" + path + "\"");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

    }


    public static void main(String[] args) {
        var path = args[0];
        WriteRandomSchema(path);
        try {
            var wordNetInterface = new WordNetInterface();
            var syns = wordNetInterface.getSynonymes("dog");
            System.out.println("synonyms for DOG:");
            for (String s : syns) {
                System.out.println(s);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}