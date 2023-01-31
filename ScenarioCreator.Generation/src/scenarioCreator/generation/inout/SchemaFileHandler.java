package scenarioCreator.generation.inout;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import scenarioCreator.data.Schema;
import scenarioCreator.generation.processing.tree.SchemaAsResult;

import java.io.IOException;
import java.nio.file.Path;

public class SchemaFileHandler {

    public static void save(SchemaAsResult sar, Path path) throws IOException {
        final var mapper = new ObjectMapper(new YAMLFactory());
        mapper.writeValue(path.toFile(), sar);
    }

    public static void save(Schema schema, Path path) throws IOException {
        final var mapper = new ObjectMapper(new YAMLFactory());
        mapper.writeValue(path.toFile(), schema);
    }

    public static SchemaAsResult load(Path path) throws IOException {
        final var mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(path.toFile(), SchemaAsResult.class);
    }
}
