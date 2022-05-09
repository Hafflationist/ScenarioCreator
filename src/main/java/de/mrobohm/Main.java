package de.mrobohm;

import de.mrobohm.inout.SchemaFileHandler;

import java.io.IOException;
import java.util.Random;

public class Main {



    public static void main(String[] args) {
        var path = args[0];
        var random = new Random();
        var schema = RandomSchemaGenerator.generateRandomSchema(random);
        try {
            SchemaFileHandler.save(schema, path);
            System.out.println("Saved schema in \"" + path + "\"");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}