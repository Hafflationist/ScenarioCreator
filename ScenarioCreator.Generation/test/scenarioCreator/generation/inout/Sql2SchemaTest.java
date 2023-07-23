package scenarioCreator.generation.inout;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class Sql2SchemaTest {

    @Test
    void convert() {
        // --- Arrange
        final var sql = """
                CREATE TABLE `roles` (
                  `actor_id` int(11) NOT NULL,
                  `movie_id` int(11) NOT NULL,
                  `role` varchar(100) NOT NULL,
                  PRIMARY KEY (`actor_id`,`movie_id`,`role`),
                  KEY `idx_actor_id` (`actor_id`),
                  KEY `idx_movie_id` (`movie_id`),
                  CONSTRAINT `roles_ibfk_1` FOREIGN KEY (`movie_id`) REFERENCES `movies` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
                  CONSTRAINT `roles_ibfk_2` FOREIGN KEY (`actor_id`) REFERENCES `actors` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
                CREATE TABLE `movies_genres` (
                  `movie_id` int(11) NOT NULL,
                  `genre` varchar(100) NOT NULL,
                  PRIMARY KEY (`movie_id`,`genre`),
                  KEY `movie_id` (`movie_id`),
                  CONSTRAINT `movies_genres_ibfk_1` FOREIGN KEY (`movie_id`) REFERENCES `movies` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
                CREATE TABLE `movies` (
                  `id` int(11) NOT NULL DEFAULT 0,
                  `name` varchar(100) DEFAULT NULL,
                  `year` int(11) DEFAULT NULL,
                  `rank` float DEFAULT NULL,
                  PRIMARY KEY (`id`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
                CREATE TABLE `actors` (
                  `id` int(11) NOT NULL DEFAULT 0,
                  `first_name` varchar(100) DEFAULT NULL,
                  `last_name` varchar(100) DEFAULT NULL,
                  `gender` char(1) DEFAULT NULL,
                  `film_count` int(11) DEFAULT 0,
                  PRIMARY KEY (`id`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
                CREATE TABLE `directors` (
                  `id` int(11) NOT NULL DEFAULT 0,
                  `first_name` varchar(100) DEFAULT NULL,
                  `last_name` varchar(100) DEFAULT NULL,
                  PRIMARY KEY (`id`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
                CREATE TABLE `directors_genres` (
                  `director_id` int(11) DEFAULT NULL,
                  `genre` varchar(100) DEFAULT NULL,
                  `prob` float DEFAULT NULL,
                  KEY `idx_director_id` (`director_id`),
                  CONSTRAINT `directors_genres_ibfk_1` FOREIGN KEY (`director_id`) REFERENCES `directors` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
                CREATE TABLE `movies_directors` (
                  `director_id` int(11) NOT NULL,
                  `movie_id` int(11) NOT NULL,
                  PRIMARY KEY (`director_id`,`movie_id`),
                  KEY `idx_director_id` (`director_id`),
                  KEY `idx_movie_id` (`movie_id`),
                  CONSTRAINT `movies_directors_ibfk_1` FOREIGN KEY (`movie_id`) REFERENCES `movies` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
                  CONSTRAINT `movies_directors_ibfk_2` FOREIGN KEY (`director_id`) REFERENCES `directors` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci
                """;

        // --- Act
        final var result = Sql2Schema.convert(sql);

        // --- Assert
        Assertions.assertTrue(result.isPresent());
        final var schema = result.get();
        Assertions.assertEquals(7, schema.tableSet().size());
    }
}