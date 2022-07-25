package de.mrobohm.data.column;

import org.jetbrains.annotations.NotNull;

import java.util.Random;

public record DataType(DataTypeEnum dataTypeEnum, boolean isNullable) implements Comparable<DataType> {

    public static DataType getRandom(Random random) {
        var dataTypeEnum = DataTypeEnum.getRandom(random);
        return new DataType(dataTypeEnum, random.nextBoolean());
    }

    public DataType withDataTypeEnum(DataTypeEnum newDataTypeEnum) {
        return new DataType(newDataTypeEnum, isNullable);
    }

    public DataType withIsNullable(boolean newIsNullable) {
        return new DataType(dataTypeEnum, newIsNullable);
    }

    @Override
    public int compareTo(@NotNull DataType dt) {
        return this.toString().compareTo(dt.toString());
    }
}