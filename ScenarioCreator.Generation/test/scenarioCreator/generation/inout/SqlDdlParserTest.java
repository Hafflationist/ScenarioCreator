package scenarioCreator.generation.inout;

import org.junit.jupiter.api.Test;
import scenarioCreator.generation.processing.integrity.IntegrityChecker;

import static org.junit.jupiter.api.Assertions.*;

class SqlDdlParserTest {

    @Test
    void parse() {
        // --- Arrange
        final var testStr = """
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
        final var tokenBlock = SqlDdlLexer.groupTokensInner(SqlDdlLexer.tokenize(testStr)).first();

        // --- Act
        final var schema = SqlDdlParser.parse(tokenBlock).get();

        // --- Assert
        IntegrityChecker.assertValidSchema(schema);
    }
}
