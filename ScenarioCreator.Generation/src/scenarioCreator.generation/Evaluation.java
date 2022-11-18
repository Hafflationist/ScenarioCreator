package scenarioCreator.generation;

import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.Nullable;
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
import scenarioCreator.generation.heterogeneity.constraintBased.CheckNumericalBasedDistanceMeasure;
import scenarioCreator.generation.heterogeneity.constraintBased.FunctionalDependencyBasedDistanceMeasure;
import scenarioCreator.generation.heterogeneity.linguistic.LinguisticDistanceMeasure;
import scenarioCreator.generation.heterogeneity.structural.ted.Ted;
import scenarioCreator.generation.processing.Scenario;
import scenarioCreator.generation.processing.ScenarioCreator;
import scenarioCreator.generation.processing.integrity.IdentificationNumberCalculator;
import scenarioCreator.generation.processing.integrity.IntegrityChecker;
import scenarioCreator.generation.processing.preprocessing.SemanticSaturation;
import scenarioCreator.generation.processing.transformations.SingleTransformationExecutor;
import scenarioCreator.generation.processing.transformations.TransformationCollection;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.Translation;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.GermaNetInterface;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.UnifiedLanguageCorpus;
import scenarioCreator.generation.processing.transformations.linguistic.helpers.biglingo.WordNetInterface;
import scenarioCreator.generation.processing.tree.DistanceDefinition;
import scenarioCreator.generation.processing.tree.DistanceMeasures;
import scenarioCreator.generation.processing.tree.Forester;
import scenarioCreator.utils.SSet;

import javax.xml.stream.XMLStreamException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public final class Evaluation {
    private Evaluation() {
    }

    public static Schema getInitSchema() {
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
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1031), SSet.of()),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1040), SSet.of()),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1050), SSet.of())
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
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1021), SSet.of()),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1030), SSet.of())
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
                                                SSet.of(new ColumnConstraintForeignKey(new IdSimple(1010), SSet.of()))
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1022),
                                                new StringPlusNaked("Hotel", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintUnique(new IdSimple(10220)),
                                                        new ColumnConstraintForeignKey(new IdSimple(1132), SSet.of())
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1023),
                                                new StringPlusNaked("Room", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintUnique(new IdSimple(10220)),
                                                        new ColumnConstraintForeignKey(new IdSimple(1131), SSet.of())
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
                                                SSet.of(new ColumnConstraintForeignKey(new IdSimple(1010), SSet.of()))
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1031),
                                                new StringPlusNaked("Event", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(new ColumnConstraintForeignKey(new IdSimple(1000), SSet.of()))
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
                                                        new ColumnConstraintForeignKey(new IdSimple(1000), SSet.of()),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1100), SSet.of())
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
                                                        new ColumnConstraintForeignKey(new IdSimple(1000), SSet.of()),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1060), SSet.of()),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1140), SSet.of()),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1150), SSet.of())
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
                                                SSet.of(new ColumnConstraintForeignKey(new IdSimple(1050), SSet.of()))
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
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1080), SSet.of()),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1092), SSet.of()),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1125), SSet.of())
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
                                                        new ColumnConstraintForeignKey(new IdSimple(1070), SSet.of())
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
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1101), SSet.of()),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1111), SSet.of()),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1112), SSet.of())
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
                                                        new ColumnConstraintForeignKey(new IdSimple(1070), SSet.of()),
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
                                                        new ColumnConstraintForeignKey(new IdSimple(1040), SSet.of())
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1101),
                                                new StringPlusNaked("PID", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintForeignKey(new IdSimple(1090), SSet.of())
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
                                                        new ColumnConstraintForeignKey(new IdSimple(1090), SSet.of())
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1112),
                                                new StringPlusNaked("Destination", Language.English),
                                                new DataType(DataTypeEnum.INT32, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintForeignKey(new IdSimple(1090), SSet.of())
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
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1132), SSet.of()),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1141), SSet.of()),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1151), SSet.of())
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
                                                        new ColumnConstraintForeignKey(new IdSimple(1070), SSet.of())
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
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1023), SSet.of())
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1132),
                                                new StringPlusNaked("Hotel", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintForeignKey(new IdSimple(1121), SSet.of()),
                                                        new ColumnConstraintForeignKeyInverse(new IdSimple(1022), SSet.of())
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
                                                        new ColumnConstraintForeignKey(new IdSimple(1050), SSet.of())
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1141),
                                                new StringPlusNaked("Hotel", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintPrimaryKey(new IdSimple(11400)),
                                                        new ColumnConstraintForeignKey(new IdSimple(1121), SSet.of())
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
                                                        new ColumnConstraintForeignKey(new IdSimple(1050), SSet.of())
                                                )
                                        ),
                                        new ColumnLeaf(
                                                new IdSimple(1151),
                                                new StringPlusNaked("Hotel", Language.English),
                                                new DataType(DataTypeEnum.NVARCHAR, false),
                                                ColumnContext.getDefault(),
                                                SSet.of(
                                                        new ColumnConstraintPrimaryKey(new IdSimple(11500)),
                                                        new ColumnConstraintForeignKey(new IdSimple(1121), SSet.of())
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
        return initSchema;
    }

    private static FunctionalDependency getFd(int left, int right) {
        return new FunctionalDependency(SSet.of(new IdSimple(left)), SSet.of(new IdSimple(right)));
    }

    private static FunctionalDependency getFd(Set<Integer> left, Set<Integer> right) {
        final var leftId = left.stream().map(n -> (Id) new IdSimple(n)).collect(Collectors.toCollection(TreeSet::new));
        final var rightId = right.stream().map(n -> (Id) new IdSimple(n)).collect(Collectors.toCollection(TreeSet::new));
        return new FunctionalDependency(leftId, rightId);
    }

    private static Optional<Scenario> runForesterInner(
            FullConfiguration config, String pathStr, @Nullable Integer seedOpt, UnifiedLanguageCorpus ulc, GermaNetInterface gni
    ) {
        try {
            // clean directory
            FileUtils.cleanDirectory(Path.of(pathStr, "scenario").toFile());

            // Manage randomness and seed
            final var metaRandom = new Random();
            final var seed = (seedOpt == null) ? metaRandom.nextInt() : seedOpt;
//            System.out.println("Seed: " + seed);
            final var random = new Random(seed);
            final var seedFile = Path.of(pathStr, "scenario/seed.txt").toFile();
            try (final var writer = new FileWriter(seedFile)) {
                writer.write(Integer.toString(seed));
            }

            // calc
            final var ss = new SemanticSaturation(ulc);
            final var schemaNaked = RandomSchemaGenerator.generateRandomSchema(
                    random, 3, 3, gni::pickRandomEnglishWord
            );
            assert !schemaNaked.tableSet().isEmpty();
            final var schema = ss.saturateSemantically(schemaNaked);

            final var allIdList = IdentificationNumberCalculator.getAllIds(schema, false).toList();
            final var nonUniqueIdSet = allIdList.stream()
                    .filter(id -> allIdList.stream().filter(id2 -> Objects.equals(id2, id)).count() >= 2)
                    .collect(Collectors.toCollection(TreeSet::new));
            if (!nonUniqueIdSet.isEmpty()) {
                return Optional.empty(); // Generation of start schema broken... (skip and forget)
            }

            final var distanceMeasures = new DistanceMeasures(
                    (s1, s2) -> {
                        try {
                            return Ted.calculateDistanceRelative(s1, s2);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    },
                    (s1, s2) -> LinguisticDistanceMeasure.calculateDistanceToRootRelative(s1, s2, ulc::semanticDiff),
                    (s1, s2) -> {
                        final var numDist = CheckNumericalBasedDistanceMeasure.calculateDistanceRelative(s1, s2);
                        final var regDist = CheckNumericalBasedDistanceMeasure.calculateDistanceRelative(s1, s2);
                        final var funDist = FunctionalDependencyBasedDistanceMeasure.calculateDistanceRelative(s1, s2);
                        return (numDist + regDist + funDist) / 3.0;
                    },
                    (__, ___) -> 0.0
            );

            final var translation = new Translation(ulc);
            final var creator = new ScenarioCreator(config.dd, ((validDefinition, targetDefinition) -> new Forester(
                    new SingleTransformationExecutor(ss),
                    new TransformationCollection(ulc, translation),
                    distanceMeasures,
                    validDefinition,
                    targetDefinition,
                    config.treeSteps
            )));
//            System.out.println("Preparations finished (rnd: " + random.nextInt(1000) + ")");
//            System.out.println("Scenario created!");
            return Optional.of(creator.create(schema, config.scenarioSize, config.newChildren(), random));
        } catch (IOException e) {
            System.out.println(e.getMessage());
            return Optional.empty();
        }
    }

    private static void runForester(String path) throws XMLStreamException, IOException {
        final var germanet = new GermaNetInterface();
        final var ulc = new UnifiedLanguageCorpus(Map.of(Language.German, germanet, Language.English, new WordNetInterface()));
        final var config = new FullConfiguration(DistanceDefinition.getDefault(0.2, 0.7), 5, 16, 3);
        runForesterInner(config, path, 38, ulc, germanet);
//        for (int i = 38; i < Integer.MAX_VALUE; i++) {
//            System.out.println("Starte Anlauf " + i + "...");
//            testForesterInner(path, i, ulc, germanet);
//            System.out.println("Anlauf " + i + " vollständig");
//        }
    }

    private record FullConfiguration(DistanceDefinition dd, int scenarioSize, int treeSteps, int newChildren) {
    }
}
