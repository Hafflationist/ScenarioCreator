package scenarioCreator.generation.evaluation;

import scenarioCreator.data.Context;
import scenarioCreator.data.Language;
import scenarioCreator.data.Schema;
import scenarioCreator.data.column.DataType;
import scenarioCreator.data.column.DataTypeEnum;
import scenarioCreator.data.column.constraint.*;
import scenarioCreator.data.column.constraint.numerical.CheckConjunction;
import scenarioCreator.data.column.constraint.numerical.CheckPrimitive;
import scenarioCreator.data.column.constraint.numerical.ComparisonType;
import scenarioCreator.data.column.context.ColumnContext;
import scenarioCreator.data.column.nesting.ColumnLeaf;
import scenarioCreator.data.dataset.Value;
import scenarioCreator.data.identification.Id;
import scenarioCreator.data.identification.IdSimple;
import scenarioCreator.data.primitives.StringPlusNaked;
import scenarioCreator.data.table.FunctionalDependency;
import scenarioCreator.data.table.Table;
import scenarioCreator.generation.processing.integrity.IntegrityChecker;
import scenarioCreator.generation.processing.preprocessing.SemanticSaturation;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import scenarioCreator.utils.SSet;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public final class Init {
    private Init() {
    }

    private static FunctionalDependency getFd(int left, int right) {
        return new FunctionalDependency(SSet.of(new IdSimple(left)), SSet.of(new IdSimple(right)));
    }

    private static FunctionalDependency getFd(Set<Integer> left, Set<Integer> right) {
        final var leftId = left.stream().map(n -> (Id) new IdSimple(n)).collect(Collectors.toCollection(TreeSet::new));
        final var rightId = right.stream().map(n -> (Id) new IdSimple(n)).collect(Collectors.toCollection(TreeSet::new));
        return new FunctionalDependency(leftId, rightId);
    }

    public static Schema getInitSchema(UnifiedLanguageCorpus ulc) {
        final var initSchema = new Schema(
                new IdSimple(0),
                new StringPlusNaked("HotelEvent", Language.Mixed),
                Context.getDefault(),
                SSet.of(
                        new Table(
                                new IdSimple(100),
                                new StringPlusNaked("Event", Language.English),
                                List.of(
                                        new ColumnLeaf(
                                                new IdSimple(1000),
                                                new StringPlusNaked("Name", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintPrimaryKey(new IdSimple(10000)),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1031)),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1040)),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1050))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1001),
                                                new StringPlusNaked("Description", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of()
                                        )
                                ),
                                Context.getDefault(),
                                SSet.of(),
                                SSet.of()
                        ),
                        new Table(
                                new IdSimple(101),
                                new StringPlusNaked("Guest", Language.English),
                                List.of(
                                        new ColumnLeaf(
                                                new IdSimple(1010),
                                                new StringPlusNaked("GID", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintPrimaryKey(new IdSimple(10100)),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1021)),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1030))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1011),
                                                new StringPlusNaked("FirstName", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of()
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1012),
                                                new StringPlusNaked("LastName", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of()
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1013),
                                                new StringPlusNaked("Birthday", Language.English),
                                                new DataType(DataTypeEnum.DATETIME, false),
                                                ColumnContext.getDefault(),
                                                SSet.of()
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1014),
                                                new StringPlusNaked("Age", Language.English),
                                                new DataType(DataTypeEnum.FLOAT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(new ColumnConstraintCheckNumerical(new CheckPrimitive(ComparisonType.GreaterEquals, 18)))
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1015),
                                                new StringPlusNaked("Nationality", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of()
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1016),
                                                new StringPlusNaked("NeedsVisa", Language.English),
                                                new DataType(DataTypeEnum.INT1, false),
                                                SSet.of(new Value("NeedsVisa"), new Value("DoesNotNeedVisa")),
                                                ColumnContext.getDefault(),
                                                SSet.of()
                                        )
                                ),
                                Context.getDefault(),
                                SSet.of(),
                                SSet.of(getFd(1013, 1014), getFd(1015, 1016))
                        ),
                        new Table(
                                new IdSimple(102),
                                new StringPlusNaked("BookingRoom", Language.English),
                                List.of(
                                        new ColumnLeaf(
                                                new IdSimple(1020),
                                                new StringPlusNaked("BID", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(new ColumnConstraintPrimaryKey(new IdSimple(10200)))
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1021),
                                                new StringPlusNaked("Guest", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(new ColumnConstraintForeignKey(new IdSimple(1010)))
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1022),
                                                new StringPlusNaked("Hotel", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintUnique(new IdSimple(10220)),
                                                        new ColumnConstraintForeignKey(new IdSimple(1132))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1023),
                                                new StringPlusNaked("Room", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintUnique(new IdSimple(10220)),
                                                        new ColumnConstraintForeignKey(new IdSimple(1131))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1024),
                                                new StringPlusNaked("Date", Language.English),
                                                new DataType(DataTypeEnum.DATETIME, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(new ColumnConstraintUnique(new IdSimple(10220)))
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1025),
                                                new StringPlusNaked("NumberPersons", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of()
                                        )
                                ),
                                Context.getDefault(),
                                SSet.of(),
                                SSet.of(getFd(Set.of(1022, 1023, 1024), Set.of(1020, 1021, 1025)))
                        ),
                        new Table(
                                new IdSimple(103),
                                new StringPlusNaked("BookingEvent", Language.English),
                                List.of(
                                        new ColumnLeaf(
                                                new IdSimple(1030),
                                                new StringPlusNaked("Guest", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(new ColumnConstraintForeignKey(new IdSimple(1010)))
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1031),
                                                new StringPlusNaked("Event", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(new ColumnConstraintForeignKey(new IdSimple(1000)))
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1032),
                                                new StringPlusNaked("Date", Language.English),
                                                new DataType(DataTypeEnum.DATETIME, false),
                                                ColumnContext.getDefault(),
                                                SSet.of()
                                        )
                                ),
                                Context.getDefault(),
                                SSet.of(),
                                SSet.of()
                        ),
                        new Table(
                                new IdSimple(104),
                                new StringPlusNaked("Excursion", Language.English),
                                List.of(
                                        new ColumnLeaf(
                                                new IdSimple(1040),
                                                new StringPlusNaked("Event", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintPrimaryKey(new IdSimple(10400)),
                                                        new ColumnConstraintForeignKey(new IdSimple(1000)),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1100))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1041),
                                                new StringPlusNaked("Price-USD", Language.English),
                                                new DataType(DataTypeEnum.DECIMAL, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintCheckNumerical(new CheckPrimitive(ComparisonType.GreaterEquals, 0))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1042),
                                                new StringPlusNaked("Capacity", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of()
                                        )
                                ),
                                Context.getDefault(),
                                SSet.of(),
                                SSet.of()
                        ),
                        new Table(
                                new IdSimple(105),
                                new StringPlusNaked("EveningProgram", Language.English),
                                List.of(
                                        new ColumnLeaf(
                                                new IdSimple(1050),
                                                new StringPlusNaked("Event", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintForeignKey(new IdSimple(1000)),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1060)),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1140)),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1150))
                                                )
                                        )
                                ),
                                Context.getDefault(),
                                SSet.of(),
                                SSet.of()
                        ),
                        new Table(
                                new IdSimple(106),
                                new StringPlusNaked("Activity", Language.English),
                                List.of(
                                        new ColumnLeaf(
                                                new IdSimple(1060),
                                                new StringPlusNaked("EveningProgram", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(new ColumnConstraintForeignKey(new IdSimple(1050)))
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1061),
                                                new StringPlusNaked("Activity", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of()
                                        )
                                ),
                                Context.getDefault(),
                                SSet.of(),
                                SSet.of()
                        ),
                        new Table(
                                new IdSimple(107),
                                new StringPlusNaked("City", Language.English),
                                List.of(
                                        new ColumnLeaf(
                                                new IdSimple(1070),
                                                new StringPlusNaked("CID", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintPrimaryKey(new IdSimple(10700)),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1080)),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1092)),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1125))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1071),
                                                new StringPlusNaked("Name", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintUnique(new IdSimple(10710))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1072),
                                                new StringPlusNaked("Country", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintUnique(new IdSimple(10710))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1073),
                                                new StringPlusNaked("Population", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintCheckNumerical(new CheckPrimitive(ComparisonType.GreaterThan, 0))
                                                )
                                        )
                                ),
                                Context.getDefault(),
                                SSet.of(),
                                SSet.of(getFd(Set.of(1071, 1072), Set.of(1070, 1073)))
                        ),
                        new Table(
                                new IdSimple(108),
                                new StringPlusNaked("ZIPCodes", Language.English),
                                List.of(
                                        new ColumnLeaf(
                                                new IdSimple(1080),
                                                new StringPlusNaked("City", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintPrimaryKey(new IdSimple(10800)),
                                                        new ColumnConstraintForeignKey(new IdSimple(1070))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1081),
                                                new StringPlusNaked("ZIP", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintPrimaryKey(new IdSimple(10800))
                                                )
                                        )
                                ),
                                Context.getDefault(),
                                SSet.of(),
                                SSet.of()
                        ),
                        new Table(
                                new IdSimple(109),
                                new StringPlusNaked("PointOfInterest", Language.English),
                                List.of(
                                        new ColumnLeaf(
                                                new IdSimple(1090),
                                                new StringPlusNaked("PID", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintPrimaryKey(new IdSimple(10900)),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1101)),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1111)),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1112))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1091),
                                                new StringPlusNaked("Name", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintUnique(new IdSimple(10910))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1092),
                                                new StringPlusNaked("City", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintForeignKey(new IdSimple(1070)),
                                                        new ColumnConstraintUnique(new IdSimple(10910))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1093),
                                                new StringPlusNaked("Opening (PST)", Language.English),
                                                new DataType(DataTypeEnum.DATETIME, false),
                                                ColumnContext.getDefault(),
                                                SSet.of()
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1094),
                                                new StringPlusNaked("Closing (PST)", Language.English),
                                                new DataType(DataTypeEnum.DATETIME, false),
                                                ColumnContext.getDefault(),
                                                SSet.of()
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1095),
                                                new StringPlusNaked("Inside", Language.English),
                                                new DataType(DataTypeEnum.INT1, false),
                                                SSet.of(new Value("inside"), new Value("outside")),
                                                ColumnContext.getDefault(),
                                                SSet.of()
                                        )
                                ),
                                Context.getDefault(),
                                SSet.of(),
                                SSet.of()
                        ),
                        new Table(
                                new IdSimple(110),
                                new StringPlusNaked("HasDestination", Language.English),
                                List.of(
                                        new ColumnLeaf(
                                                new IdSimple(1100),
                                                new StringPlusNaked("Excursion", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintPrimaryKey(new IdSimple(11000)),
                                                        new ColumnConstraintForeignKey(new IdSimple(1040))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1101),
                                                new StringPlusNaked("PID", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintForeignKey(new IdSimple(1090))
                                                )
                                        )
                                ),
                                Context.getDefault(),
                                SSet.of(),
                                SSet.of()
                        ),
                        new Table(
                                new IdSimple(111),
                                new StringPlusNaked("Reachable", Language.English),
                                List.of(
                                        new ColumnLeaf(
                                                new IdSimple(1111),
                                                new StringPlusNaked("Start", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintForeignKey(new IdSimple(1090))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1112),
                                                new StringPlusNaked("Destination", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintForeignKey(new IdSimple(1090))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1113),
                                                new StringPlusNaked("Duration [min]", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintCheckNumerical(new CheckPrimitive(ComparisonType.GreaterThan, 0))
                                                )
                                        )
                                ),
                                Context.getDefault(),
                                SSet.of(),
                                SSet.of()
                        ),
                        new Table(
                                new IdSimple(112),
                                new StringPlusNaked("Hotel", Language.English),
                                List.of(
                                        new ColumnLeaf(
                                                new IdSimple(1121),
                                                new StringPlusNaked("Name", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintPrimaryKey(new IdSimple(11210)),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1132)),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1141)),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1151))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1122),
                                                new StringPlusNaked("Street", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of()
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1123),
                                                new StringPlusNaked("Housenumber", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of()
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1124),
                                                new StringPlusNaked("ZIP", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of()
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1125),
                                                new StringPlusNaked("City", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintForeignKey(new IdSimple(1070))
                                                )
                                        )
                                ),
                                Context.getDefault(),
                                SSet.of(),
                                SSet.of()
                        ),
                        new Table(
                                new IdSimple(113),
                                new StringPlusNaked("Room", Language.English),
                                List.of(
                                        new ColumnLeaf(
                                                new IdSimple(1131),
                                                new StringPlusNaked("Number", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintPrimaryKey(new IdSimple(11310)),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1023))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1132),
                                                new StringPlusNaked("Hotel", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintForeignKey(new IdSimple(1121)),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1022))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1133),
                                                new StringPlusNaked("NumberOfBeds", Language.English),
                                                new DataType(DataTypeEnum.INT8, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintCheckNumerical(new CheckConjunction(SSet.of(
                                                                new CheckPrimitive(ComparisonType.GreaterEquals, 1),
                                                                new CheckPrimitive(ComparisonType.LowerEquals, 5)
                                                        )))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1134),
                                                new StringPlusNaked("PriceUSD", Language.English),
                                                new DataType(DataTypeEnum.DECIMAL, false),
                                                ColumnContext.getDefault(),
                                                SSet.of()
                                        )
                                ),
                                Context.getDefault(),
                                SSet.of(),
                                SSet.of()
                        ),
                        new Table(
                                new IdSimple(114),
                                new StringPlusNaked("Advertised", Language.English),
                                List.of(
                                        new ColumnLeaf(
                                                new IdSimple(1140),
                                                new StringPlusNaked("EveningProgram", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintPrimaryKey(new IdSimple(11400)),
                                                        new ColumnConstraintForeignKey(new IdSimple(1050))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1141),
                                                new StringPlusNaked("Hotel", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintPrimaryKey(new IdSimple(11400)),
                                                        new ColumnConstraintForeignKey(new IdSimple(1121))
                                                )
                                        )
                                ),
                                Context.getDefault(),
                                SSet.of(),
                                SSet.of()
                        ),
                        new Table(
                                new IdSimple(115),
                                new StringPlusNaked("TakesPlace", Language.English),
                                List.of(
                                        new ColumnLeaf(
                                                new IdSimple(1150),
                                                new StringPlusNaked("EveningProgram", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintPrimaryKey(new IdSimple(11500)),
                                                        new ColumnConstraintForeignKey(new IdSimple(1050))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1151),
                                                new StringPlusNaked("Hotel", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintPrimaryKey(new IdSimple(11500)),
                                                        new ColumnConstraintForeignKey(new IdSimple(1121))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1152),
                                                new StringPlusNaked("Date", Language.English),
                                                new DataType(DataTypeEnum.DATETIME, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintPrimaryKey(new IdSimple(11500))
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1153),
                                                new StringPlusNaked("StartTime [PST]", Language.English),
                                                new DataType(DataTypeEnum.DATETIME, false),
                                                ColumnContext.getDefault(),
                                                SSet.of()
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1154),
                                                new StringPlusNaked("EndTime [PST]", Language.English),
                                                new DataType(DataTypeEnum.DATETIME, false),
                                                ColumnContext.getDefault(),
                                                SSet.of()
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1155),
                                                new StringPlusNaked("Capacity", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of()
                                        )
                                ),
                                Context.getDefault(),
                                SSet.of(),
                                SSet.of()
                        )
                )
        );
        IntegrityChecker.assertValidSchema(initSchema);
        final var ss = new SemanticSaturation(ulc);
        final var semanticInitSchema = ss.saturateSemantically(initSchema);
        IntegrityChecker.assertValidSchema(semanticInitSchema);
        return semanticInitSchema;
    }
}
