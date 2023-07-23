package scenarioCreator.generation.inout;

import scenarioCreator.data.Context;
import scenarioCreator.data.Language;
import scenarioCreator.data.Schema;
import scenarioCreator.data.column.DataType;
import scenarioCreator.data.column.DataTypeEnum;
import scenarioCreator.data.column.constraint.ColumnConstraint;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKey;
import scenarioCreator.data.column.constraint.ColumnConstraintForeignKeyInverse;
import scenarioCreator.data.column.constraint.ColumnConstraintPrimaryKey;
import scenarioCreator.data.column.context.ColumnContext;
import scenarioCreator.data.column.nesting.Column;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.identification.IdSimple;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.data.table.Table;
import scenarioCreator.generation.inout.sqlToken.*;
import scenarioCreator.utils.Pair;
import scenarioCreator.utils.SSet;
import scenarioCreator.utils.StreamExtensions;

import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SqlDdlParser {

    public static Optional<Schema> parse(SqlTokenBlock tokenBlock) {
        return tokensToSchema("MeinSchema", tokenBlock);
    }

    private static Optional<IdSimple> getColumnIdFromNameInBraces(List<SqlTokenMaybeWithBlock> tokens) {
        return tokens.stream()
                .filter(t -> t instanceof SqlTokenBlock)
                .findFirst()
                .map(t -> (SqlTokenBlock) t)
                .filter(t -> t.block().size() == 1)
                .map(t -> (TokenIdentifier) t.block().get(0))
                .map(TokenIdentifier::name)
                .map(SqlDdlParser::nameToId);
    }

    private static List<Pair<IdSimple, IdSimple>> getForeignKeyConstraints(
            SqlTokenBlock hollisticBlock
    ) {
        return StreamExtensions.split(hollisticBlock.block().stream(), new TokenSemicolon())
                .flatMap(block -> {
                    final var tableIdOpt = findIdentifier(block)
                            .map(StringPlusNaked::rawString)
                            .map(SqlDdlParser::nameToId);
                    if (tableIdOpt.isEmpty()) return Stream.of();
                    final var innerBlockOpt = block.stream()
                            .filter(t -> t instanceof SqlTokenBlock)
                            .map(t -> (SqlTokenBlock) t).findFirst();
                    if (innerBlockOpt.isEmpty()) return Stream.of();

                    return StreamExtensions.split(innerBlockOpt.get().block().stream(), new TokenComma())
                            .map(line -> StreamExtensions.split(line.stream(), new TokenReferences()))
                            .map(Stream::toList)
                            .filter(lineParts -> lineParts.size() == 2)
                            .map(lineParts -> {
                                final var firstPart = lineParts.get(0);
                                final var fromIdOpt = getColumnIdFromNameInBraces(firstPart)
                                        .map(columnId -> combineIds(columnId, tableIdOpt.get()));
                                if (fromIdOpt.isEmpty()) return Optional.<Pair<IdSimple, IdSimple>>empty();
                                final var secondPart = lineParts.get(1);
                                return getColumnIdFromNameInBraces(secondPart)
                                        .flatMap(toColumnId ->
                                                findIdentifier(secondPart)
                                                        .map(StringPlusNaked::rawString)
                                                        .map(SqlDdlParser::nameToId)
                                                        .map(tableId -> combineIds(toColumnId, tableId)))
                                        .map(toId -> new Pair<>(fromIdOpt.get(), toId));
                            })
                            .filter(Optional::isPresent)
                            .map(Optional::get);
                })
                .toList();
    }

    private static IdSimple nameToId(String name) {
        return new IdSimple(name.hashCode());
    }

    private static IdSimple combineIds(IdSimple ids1, IdSimple ids2) {
        return new IdSimple(ids1.number() ^ ids2.number());
    }

    private static Optional<StringPlusNaked> findIdentifier(List<SqlTokenMaybeWithBlock> tokens) {
        return tokens.stream()
                .filter(t -> t instanceof TokenIdentifier)
                .map(t -> (TokenIdentifier) t)
                .findFirst()
                .map(TokenIdentifier::name)
                .map(identifier -> new StringPlusNaked(identifier, Language.Mixed));
    }

    private static DataType tokenToDataType(TokenDataType token, boolean isNullable) {
        final var dte = switch (token.content().toLowerCase()) {
            case "bit" -> DataTypeEnum.INT1;
            case "tiny", "tinyint" -> DataTypeEnum.INT8;
            case "smallint" -> DataTypeEnum.INT16;
            case "int" -> DataTypeEnum.INT32;
            case "bigint" -> DataTypeEnum.INT64;
            case "float" -> DataTypeEnum.FLOAT32;
            case "timestamp", "date" -> DataTypeEnum.DATETIME;
            case "varchar", "nvarchar" -> DataTypeEnum.NVARCHAR;
            default -> DataTypeEnum.DECIMAL;
        };
        return new DataType(dte, isNullable);
    }

    private static Optional<Column> tokensToColumn(
            IdSimple tableId,
            List<SqlTokenMaybeWithBlock> tokens,
            List<Pair<List<String>, ColumnConstraintPrimaryKey>> pks,
            List<Pair<IdSimple, IdSimple>> fks
    ) {
        final var isNullable = tokens.stream().noneMatch(t -> t instanceof TokenNot);
        final var tokenDataTypeOpt = tokens.stream()
                .filter(t -> t instanceof TokenDataType)
                .map(t -> (TokenDataType) t)
                .findFirst();
        if (tokenDataTypeOpt.isEmpty()) {
            return Optional.empty();
        }
        final var dataType = tokenToDataType(tokenDataTypeOpt.get(), isNullable);
        final var spnOpt = findIdentifier(tokens);
        if (spnOpt.isEmpty()) return Optional.empty();
        final var columnId = combineIds(tableId, nameToId(spnOpt.get().rawString()));

        final var constraintsPk = pks.stream()
                .filter(pair -> pair.first().stream().anyMatch(name -> name.equals(spnOpt.get().rawString())))
                .map(Pair::second)
                .map(c -> (ColumnConstraint) c)
                .collect(Collectors.toCollection(TreeSet::new));
        final var constraintsFk = fks.stream()
                .filter(pair -> pair.first().equals(columnId))
                .map(pair -> new ColumnConstraintForeignKey(pair.second()))
                .map(ccfk -> (ColumnConstraint) ccfk)
                .collect(Collectors.toCollection(TreeSet::new));
        final var constraintsFki = fks.stream()
                .filter(pair -> pair.second().equals(columnId))
                .map(pair -> new ColumnConstraintForeignKeyInverse(pair.first()))
                .map(ccfki -> (ColumnConstraint) ccfki)
                .collect(Collectors.toCollection(TreeSet::new));
        final var constraints = SSet.concat(constraintsPk, SSet.concat(constraintsFk, constraintsFki));
        return Optional.of(new ColumnLeaf(
                columnId,
                spnOpt.get(),
                dataType,
                ColumnContext.getDefault(),
                constraints
        ));
    }

    private static Pair<List<String>, ColumnConstraintPrimaryKey> tokensToPrimaryKeyConstraint(
            List<SqlTokenMaybeWithBlock> tokens
    ) {
        final var names = tokens.stream()
                .filter(t -> t instanceof TokenIdentifier)
                .map(t -> (TokenIdentifier) t)
                .map(TokenIdentifier::name)
                .toList();
        final var pkId = new IdSimple(StreamExtensions.foldLeft(names.stream(), 0, (a, b) -> a ^ b.hashCode()));
        return new Pair<>(names, new ColumnConstraintPrimaryKey(pkId));
    }

    private static Optional<Table> tokensToTable(List<SqlTokenMaybeWithBlock> tokens, List<Pair<IdSimple, IdSimple>> fks) {
        final var spnOpt = findIdentifier(tokens);
        if (spnOpt.isEmpty()) {
            return Optional.empty();
        }
        final var spn = spnOpt.get();
        final var tableId = nameToId(spn.rawString());

        final var blockOpt = tokens.stream()
                .filter(t -> t instanceof SqlTokenBlock)
                .map(t -> (SqlTokenBlock) t)
                .findFirst();
        if (blockOpt.isEmpty()) {
            return Optional.empty();
        }
        final var lines = StreamExtensions.split(blockOpt.get().block().stream(), new TokenComma()).toList();
        System.out.println();
        System.out.println("Detected lines: " + lines);
        System.out.println();
        final var pks = lines.stream()
                .filter(line -> line.stream().noneMatch(t -> t instanceof TokenPrimary))
                .map(SqlDdlParser::tokensToPrimaryKeyConstraint)
                .toList();
        final var columnList = lines.stream()
                .filter(line -> line.stream().noneMatch(t -> t instanceof TokenReferences))
                .map(line -> tokensToColumn(tableId, line, pks, fks))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toList();
        return Optional.of(new Table(tableId, spn, columnList, Context.getDefault(), SSet.of(), SSet.of()));
    }

    private static Optional<Schema> tokensToSchema(String name, SqlTokenBlock hollisticBlock) {
        final var blocks = StreamExtensions.split(hollisticBlock.block().stream(), new TokenSemicolon());
        final var spn = new StringPlusNaked(name, Language.Mixed);
        final var id = new IdSimple(0);
        final var fks = getForeignKeyConstraints(hollisticBlock);
        final var tables = blocks
                .map(block -> tokensToTable(block, fks))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toCollection(TreeSet::new));
        if (tables.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new Schema(id, spn, Context.getDefault(), tables));
    }
}
