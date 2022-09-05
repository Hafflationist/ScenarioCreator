package de.mrobohm.data.column;

import de.mrobohm.utils.SSet;

import java.util.Random;

public enum DataTypeEnum {
    FLOAT16,
    FLOAT32,
    FLOAT64,
    DECIMAL,
    INT1,
    INT8,
    INT16,
    INT32,
    INT64,
    DATETIME,
    NVARCHAR;
    // TODO: Add more

    public static DataTypeEnum getRandom(Random random) {
        final var rnd = random.nextInt(0, 10);
        return switch (rnd) {
            case 0 -> DataTypeEnum.FLOAT16;
            case 1 -> DataTypeEnum.FLOAT32;
            case 2 -> DataTypeEnum.FLOAT64;
            case 3 -> DataTypeEnum.DECIMAL;
            case 4 -> DataTypeEnum.INT1;
            case 5 -> DataTypeEnum.INT16;
            case 6 -> DataTypeEnum.INT32;
            case 7 -> DataTypeEnum.INT64;
            case 8 -> DataTypeEnum.DATETIME;
            case 9 -> DataTypeEnum.NVARCHAR;
            default -> throw new IllegalStateException("Unexpected value: " + rnd);
        };
    }

    public boolean isSmallerThan(DataTypeEnum dt) {
        return switch (this) {
            case FLOAT16 -> SSet.of(FLOAT32, FLOAT64, DECIMAL, NVARCHAR).contains(dt);
            case FLOAT32 -> SSet.of(FLOAT64, DECIMAL, NVARCHAR).contains(dt);
            case FLOAT64 -> NVARCHAR == dt;
            case DECIMAL -> SSet.of(FLOAT64, NVARCHAR).contains(dt);
            case INT1 -> SSet.of(INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, DECIMAL, NVARCHAR).contains(dt);
            case INT8 -> SSet.of(INT16, INT32, INT64, FLOAT32, FLOAT64, DECIMAL, NVARCHAR).contains(dt);
            case INT16 -> SSet.of(INT32, INT64, FLOAT32, FLOAT64, DECIMAL, NVARCHAR).contains(dt);
            case INT32 -> SSet.of(INT64, FLOAT32, FLOAT64, DECIMAL, NVARCHAR).contains(dt);
            case INT64 -> SSet.of(FLOAT64, DECIMAL, DATETIME, NVARCHAR).contains(dt);
            case DATETIME -> SSet.of(INT64, NVARCHAR).contains(dt);
            case NVARCHAR -> false;
        };
    }
}
