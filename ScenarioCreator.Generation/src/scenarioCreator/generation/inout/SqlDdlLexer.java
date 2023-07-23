package scenarioCreator.generation.inout;

import scenarioCreator.generation.inout.sqlToken.*;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.StreamExtensions;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class SqlDdlLexer {

    public static final String testInputEinfach = """
            -- legalActs.legalacts definition

            CREATE TABLE `legalacts` (
              `id` int(11) NOT NULL,
              `update` timestamp /* mariadb-5.3 */ NOT NULL DEFAULT current_timestamp(),
                PRIMARY KEY (`id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
            """;
    public static final String testInput = """
            -- legalActs.legalacts definition

            CREATE TABLE `legalacts` (
              `id` int(11) NOT NULL,
              `hash` char(32) NOT NULL,
              `update` timestamp /* mariadb-5.3 */ NOT NULL DEFAULT current_timestamp(),
              `Court` varchar(100) DEFAULT NULL,
              `CaseKind` varchar(50) DEFAULT NULL,
              `CaseNumber` smallint(6) DEFAULT NULL,
              `ActYear` smallint(6) DEFAULT NULL,
              `Judge` varchar(255) DEFAULT NULL,
              `ActKind` varchar(20) DEFAULT NULL,
              `ActNumber` smallint(6) DEFAULT NULL,
              `StartDate` date DEFAULT NULL,
              `LegalDate` date DEFAULT NULL,
              `Status` varchar(20) DEFAULT NULL,
              `ActLink` tinyint(1) NOT NULL DEFAULT 0,
              `MotiveDate` date DEFAULT NULL,
              `MotiveLink` tinyint(1) NOT NULL DEFAULT 0,
              `HighCourt` varchar(100) DEFAULT NULL,
              `OutNumber` smallint(6) DEFAULT NULL,
              `YearHigherCourt` smallint(6) DEFAULT NULL,
              `TypeOfDocument` varchar(100) DEFAULT NULL,
              `SendDate` date DEFAULT NULL,
              `ResultOfAppeal` varchar(100) DEFAULT NULL,
                PRIMARY KEY (`id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;


            -- legalActs.people definition

            CREATE TABLE `people` (
              `personId` int(11) NOT NULL AUTO_INCREMENT,
              `name` varchar(255) NOT NULL,
              `jury` tinyint(1) NOT NULL DEFAULT 0,
              `court` varchar(100) DEFAULT NULL,
                PRIMARY KEY (`personId`)
            ) ENGINE=InnoDB AUTO_INCREMENT=3790 DEFAULT CHARSET=utf8mb3;


            -- legalActs.legalact_link definition

            CREATE TABLE `legalact_link` (
              `actId1` int(11) NOT NULL,
              `actId2` int(11) NOT NULL,
                PRIMARY KEY (`actId1`,`actId2`),
                KEY `actId2` (`actId2`),
                CONSTRAINT `legalact_link_ibfk_1` FOREIGN KEY (`actId1`) REFERENCES `legalacts` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
                CONSTRAINT `legalact_link_ibfk_2` FOREIGN KEY (`actId2`) REFERENCES `legalacts` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;""";

    public static List<SqlToken> tokenize(String sql) {
        final var preparedRawString = sql
                .replace(",", " , ")
                .replace(";", " ; ")
                .replace("(", " ( ")
                .replace(")", " ) ");
        //System.out.println(preparedRawString);
        return Arrays.stream(preparedRawString.split("\n"))
                .map(line -> Arrays.stream(line.split("--")))
                .map(Stream::findFirst)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .flatMap(line -> Arrays.stream(line.split(" ")))
                .filter(word -> !word.isEmpty())
                .map(SqlDdlLexer::word2Token)
                .toList();
    }

    private static SqlToken word2Token(String word) {
        final var dataTypes = Set.of(
                "bit", "tiny", "tinyint", "smallint", "int", "bigint",
                "float",
                "timestamp", "date", "char", "varchar", "nvarchar"
        );
        if (dataTypes.contains(word)) {
            return new TokenDataType(word);
        }
        if (word.charAt(0) == '`') {
            return new TokenIdentifier(word.replace("`", ""));
        }
        return switch (word.toLowerCase()) {
            case "references" -> new TokenReferences();
            case "(" -> new TokenBraceStart();
            case ")" -> new TokenBraceEnd();
            case "," -> new TokenComma();
            case ";" -> new TokenSemicolon();
            case "*/" -> new TokenCommentEnd();
            case "/*" -> new TokenCommentStart();
            case "create" -> new TokenCreate();
            case "not" -> new TokenNot();
            case "table" -> new TokenTable();
            case String rest -> new TokenIgnore(rest);
        };
    }

    public static Pair<SqlTokenBlock, List<SqlToken>> groupTokensInner(List<SqlToken> tokens) {
        final var firstBraceOpt = tokens.stream().filter(t -> t instanceof TokenBraceStart || t instanceof TokenBraceEnd).findFirst();
        if (firstBraceOpt.isEmpty()) {
            return new Pair<>(
                    new SqlTokenBlock(tokens.stream().map(t -> (SqlTokenMaybeWithBlock) t).toList()),
                    List.of()
            );
        } else if (firstBraceOpt.get() instanceof TokenBraceEnd) {
            final var partInGroup = tokens.stream()
                    .takeWhile(t -> !(t instanceof TokenBraceEnd))
                    .map(t -> (SqlTokenMaybeWithBlock) t)
                    .toList();
            final var partBeyondGroup = tokens.stream()
                    .skip(partInGroup.size() + 1)
                    .toList();
            final var group = new SqlTokenBlock(partInGroup);
            return new Pair<>(group, partBeyondGroup);
        } else {
            final var partBeforeGroup = tokens.stream()
                    .takeWhile(t -> !(t instanceof TokenBraceStart))
                    .map(t -> (SqlTokenMaybeWithBlock) t)
                    .toList();
            final var secondPartWithNestedGroup = tokens.stream()
                    .skip(partBeforeGroup.size() + 1)
                    .toList();
            final var pair = groupTokensInner(secondPartWithNestedGroup);
            final var innerBlock = pair.first();
            final var partAfterGroup = pair.second();
            final var pairAfter = groupTokensInner(partAfterGroup);
            final var afterBlock = pairAfter.first();
            final var unprocessed = pairAfter.second();
            return new Pair<>(
                    new SqlTokenBlock(
                            Stream.concat(
                                    partBeforeGroup.stream(),
                                    StreamExtensions.prepend(afterBlock.block().stream(), innerBlock)
                            ).toList()),
                    unprocessed
            );
        }
    }
}
