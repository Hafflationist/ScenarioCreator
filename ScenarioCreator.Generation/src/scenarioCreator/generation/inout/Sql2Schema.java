package scenarioCreator.generation.inout;

import scenarioCreator.data.Schema;

import java.util.Optional;

public class Sql2Schema {

    private Sql2Schema() {
    }

    public static Optional<Schema> convert(String input) {
        System.out.println("Aggregiertes SQL: " + input);
        final var tokens = SqlDdlLexer.tokenize(input);
        final var tokenBlock = SqlDdlLexer.groupTokensInner(tokens).first();
        return SqlDdlParser.parse(tokenBlock);
    }
}
