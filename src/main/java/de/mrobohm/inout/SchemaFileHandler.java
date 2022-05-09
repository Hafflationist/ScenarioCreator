package de.mrobohm.inout;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import de.mrobohm.data.Schema;

import java.io.*;

public class SchemaFileHandler {

    public static void save(Schema schema, String path) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.writeValue(new File(path), schema);
    }

    public static Schema load(String path) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(path), Schema.class);
    }
}
