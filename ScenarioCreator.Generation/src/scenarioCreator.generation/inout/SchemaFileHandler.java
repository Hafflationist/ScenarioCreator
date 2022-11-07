package scenarioCreator.generation.inout;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import scenarioCreator.data.Schema;

import java.io.IOException;
import java.nio.file.Path;

public class SchemaFileHandler {

    public static void save(Schema schema, Path path) throws IOException {
        final var mapper = new ObjectMapper(new YAMLFactory());
        mapper.writeValue(path.toFile(), schema);
    }

    public static Schema load(Path path) throws IOException {
        final var mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(path.toFile(), Schema.class);
    }
}
