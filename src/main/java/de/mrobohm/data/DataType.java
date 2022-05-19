package de.mrobohm.data;

import java.util.Objects;
import java.util.Random;
import java.util.Set;

public enum DataType {
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

    public static DataType getRandom(Random random) {
        var rnd = random.nextInt(0, 10);
        return switch (rnd) {
            case 0 -> DataType.FLOAT16;
            case 1 -> DataType.FLOAT32;
            case 2 -> DataType.FLOAT64;
            case 3 -> DataType.DECIMAL;
            case 4 -> DataType.INT1;
            case 5 -> DataType.INT16;
            case 6 -> DataType.INT32;
            case 7 -> DataType.INT64;
            case 8 -> DataType.DATETIME;
            case 9 -> DataType.NVARCHAR;
            default -> throw new IllegalStateException("Unexpected value: " + rnd);
        };
    }

    public boolean isSmallerThan(DataType dt) {
        return switch (this) {
            case FLOAT16 -> Set.of(FLOAT32, FLOAT64, DECIMAL, NVARCHAR).contains(dt);
            case FLOAT32 -> Set.of(FLOAT64, DECIMAL, NVARCHAR).contains(dt);
            case FLOAT64 -> NVARCHAR == dt;
            case DECIMAL -> Set.of(FLOAT64, NVARCHAR).contains(dt);
            case INT1 -> Set.of(INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, DECIMAL, NVARCHAR).contains(dt);
            case INT8 -> Set.of(INT16, INT32, INT64, FLOAT32, FLOAT64, DECIMAL, NVARCHAR).contains(dt);
            case INT16 -> Set.of(INT32, INT64, FLOAT32, FLOAT64, DECIMAL, NVARCHAR).contains(dt);
            case INT32 -> Set.of(INT64, FLOAT32, FLOAT64, DECIMAL, NVARCHAR).contains(dt);
            case INT64 -> Set.of(FLOAT64, DECIMAL, DATETIME, NVARCHAR).contains(dt);
            case DATETIME -> Set.of(INT64, NVARCHAR).contains(dt);
            case NVARCHAR -> false;
        };
    }
}