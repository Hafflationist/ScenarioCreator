package scenarioCreator.generation.inout;

import scenarioCreator.data.Schema;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.table.InstancesOfTable;
import scenarioCreator.data.table.Table;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.LinguisticUtils;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.StreamExtensions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Eingabeverzeichnis {

    private Eingabeverzeichnis() {
    }

    public static Optional<Pair<Schema, List<InstancesOfTable>>> readWholeSchema(Path eingabeverzeichnis) {
        final var fileArray = new File(eingabeverzeichnis.toUri()).listFiles();
        final var schemaOpt = filesToSchema(fileArray);
        return schemaOpt.map(schema ->
                new Pair<>(
                        schema,
                        filesToInstances(fileArray, schema)
                )
        );
    }

    private static Optional<Schema> filesToSchema(File[] fileArray) {
        final var sqlString = Arrays.stream(fileArray)
                .filter(file -> file.getName().endsWith(".sql"))
                .flatMap(file -> {
                    try {
                        return Files.readAllLines(file.toPath()).stream();
                    } catch (IOException e) {
                        System.err.println("REEE: Fehler beim Lesen der .sql-Dateien!");
                        System.err.println("REEE: Fehlerhafte Datei: " + file.getName());
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.joining());
        return Sql2Schema.convert(sqlString);
    }

    private static List<InstancesOfTable> filesToInstances(File[] fileArray, Schema schema) {
        return Arrays.stream(fileArray)
                .filter(file -> file.getName().endsWith(".csv"))
                .map(file -> {
                    try {
                        final var lines = Files.readAllLines(file.toPath()).stream();
                        final var table = matchNameToTable(file.getName().split(".csv")[0], schema);
                        return new Pair<>(table, lines);
                    } catch (IOException e) {
                        System.err.println("REEE: Fehler beim Lesen der .csv-Dateien!");
                        System.err.println("REEE: Fehlerhafte Datei: " + file.getName());
                        throw new RuntimeException(e);
                    }
                })
                .map(pair -> parseInstances(pair.first(), pair.second(), schema))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toList();
    }

    private static Optional<InstancesOfTable> parseInstances(Table table, Stream<String> lines, Schema schema) {
        final var lineList = lines.toList();
        final var firstLineOpt = lineList.stream().findFirst();
        if (firstLineOpt.isEmpty()) {
            return Optional.empty();
        }
        final var idList = Arrays.stream(firstLineOpt.get().split(","))
                .map(name -> matchNameToColumnId(name, schema))
                .toList();
        final var valueListList = lineList.stream()
                .skip(1)
                .map(line -> Arrays.stream(line.split(","))
                        .map(value -> value.replace("\"", ""))
                        .toList()
                ).toList();
        // Assert correct length:
        final var hasCorrectLength = valueListList.stream()
                .allMatch(valueList -> (long) valueList.size() == idList.size());
        if (!hasCorrectLength) {
            System.err.println("REEE: Eine CSV-Datei enthält Datensätze, die nicht die gleiche Anzahl an Werten haben wie die erste Zeile!");
            return Optional.empty();
        }
        // Assertion finished
        final var valueMapList = valueListList.stream().map(valueList -> StreamExtensions
                .zip(idList.stream(), valueList.stream(), Pair::new)
                .collect(Collectors.toMap(
                        Pair::first,
                        Pair::second
                )))
                .toList();
        return Optional.of(new InstancesOfTable(table, valueMapList));
    }

    private static String reduceName(String name) {
        return name
                .replace("\"", "")
                .replace(" ", "")
                .replace("-", "")
                .replace("_", "")
                .replace(".csv", "")
                .replace(".", "")
                .toLowerCase();
    }

    private static Table matchNameToTable(String tableName, Schema schema) {
        final var reducedTableName = reduceName(tableName);
        final var tableOpt = schema.tableSet().stream()
                .filter(table-> reduceName(table.name().rawString(LinguisticUtils::merge))
                        .equals(reducedTableName)
                )
                .findFirst();
        if (tableOpt.isEmpty()) {
            System.err.println("REEE: Konnte kein SQL-DDL parsen für die korrespondierende Instanzdatei zur Tabelle " + tableName);
            System.err.println("REEE: Gesucht wurde nach der Zeichenfolge \"" + reducedTableName + "\"");
            System.err.println("REEE: Gefunden wurde nur:");
            final var names = schema.tableSet().stream()
                    .map(Table::name)
                    .map(name -> name.rawString(LinguisticUtils::merge))
                    .map(Eingabeverzeichnis::reduceName)
                    .toList();
            System.err.println(names);
            throw new InvalidParameterException("REEE");
        }
        return tableOpt.get();
    }

    private static Id matchNameToColumnId(String columnName, Schema schema) {
        final var reducedColumnName = reduceName(columnName);
        final var totalColumnList = schema.tableSet().stream()
                .flatMap(table -> table.columnList().stream())
                .toList();
        final var columnOpt = totalColumnList.stream()
                .filter(column -> reduceName(column.name().rawString(LinguisticUtils::merge))
                        .equals(reducedColumnName)
                )
                .findFirst();
        if (columnOpt.isEmpty()) {
            System.err.println("REEE: Konnte kein SQL-DDL parsen für die korrespondierende Spalte" + columnName);
            System.err.println("REEE: Gesucht wurde nach der Zeichenfolge \"" + reducedColumnName + "\"");
            System.err.println("REEE: Gefunden wurde nur:");
            final var names = schema.tableSet().stream()
                    .flatMap(table -> table.columnList().stream())
                    .map(Column::name)
                    .map(name -> name.rawString(LinguisticUtils::merge))
                    .map(Eingabeverzeichnis::reduceName)
                    .toList();
            System.err.println(names);
            throw new InvalidParameterException("REEE");
        }
        return columnOpt.get().id();
    }
}
