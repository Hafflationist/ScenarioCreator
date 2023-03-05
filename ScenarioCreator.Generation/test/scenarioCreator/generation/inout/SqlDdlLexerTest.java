package scenarioCreator.generation.inout;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scenarioCreator.generation.inout.sqlToken.*;

import java.util.List;

class SqlDdlLexerTest {

    @Test
    void tokenize() {
        // --- Arrange
        final var str = "(()((,),);)()";

        // --- Act
        final var result = SqlDdlLexer.tokenize(str);

        // --- Assert
        final var expected = List.of(
                new TokenBraceStart(),
                new TokenBraceStart(),
                new TokenBraceEnd(),
                new TokenBraceStart(),
                new TokenBraceStart(),
                new TokenComma(),
                new TokenBraceEnd(),
                new TokenComma(),
                new TokenBraceEnd(),
                new TokenSemicolon(),
                new TokenBraceEnd(),
                new TokenBraceStart(),
                new TokenBraceEnd()
        );
        Assertions.assertEquals(expected, result);
    }

    @Test
    void gatherTokens() {
        // --- Arrange
        final var tokens = List.<SqlToken>of(
                new TokenBraceStart(),
                new TokenBraceStart(),
                new TokenBraceEnd(),
                new TokenBraceStart(),
                new TokenBraceStart(),
                new TokenComma(),
                new TokenBraceEnd(),
                new TokenComma(),
                new TokenBraceEnd(),
                new TokenSemicolon(),
                new TokenBraceEnd(),
                new TokenBraceStart(),
                new TokenBraceEnd()
        );

        // --- Act
        final var pair = SqlDdlLexer.groupTokensInner(tokens);

        // --- Assert
        final var expected = new SqlTokenBlock(List.of(
                new SqlTokenBlock(List.of(
                        new SqlTokenBlock(List.of()),
                        new SqlTokenBlock(List.of(
                                new SqlTokenBlock(List.of(
                                        new TokenComma()
                                )),
                                new TokenComma()
                        )),
                        new TokenSemicolon()
                )),
                new SqlTokenBlock(List.of())
        ));
        Assertions.assertEquals(expected, pair.first());
    }
}