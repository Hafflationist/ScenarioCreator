package scenarioCreator.generation.inout;

import org.apache.commons.lang3.NotImplementedException;
import scenarioCreator.generation.inout.sqlToken.*;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class SqlDdlParser {

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
                .replace("(", " ( ")
                .replace(")", " ) ");
        //System.out.println(preparedRawString);
        final var lines = Arrays.stream(preparedRawString.split("\n"))
                .map(line -> Arrays.stream(line.split("--")))
                .map(Stream::findFirst)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .flatMap(line -> Arrays.stream(line.split(" ")))
                .filter(word -> !word.isEmpty())
                .map(SqlDdlParser::word2Token)
                .toList();

        for (var line : lines) {
            System.out.println(line);
        }
        var words = sql.replace('\n', ' ').split(" ");
        //System.out.println(Arrays.toString(words));
        throw new NotImplementedException("implement me!");
    }

    private static SqlToken word2Token(String word) {
        final var dataTypes = Set.of("int", "timestamp", "date", "char", "varchar", "nvarchar", "bit", "tiny", "tinyint", "smallint", "bigint");
        if(dataTypes.contains(word)) {
            return new TokenDataType(word);
        }
        if (word.charAt(0) == '`') {
            return new TokenIdentifier(word.replace("`", ""));
        }
        return switch (word.toLowerCase()) {
            case "," -> new TokenComma();
            case "*/" -> new TokenCommentEnd();
            case "/*" -> new TokenCommentStart();
            case "create" -> new TokenCreate();
            case "not" -> new TokenNot();
            case "table" -> new TokenTable();
            case String rest -> new TokenIgnore(rest);
        };
    }

}
