package de.mrobohm.data.dataset;

import de.mrobohm.data.column.Column;

import java.util.Map;

public record Dataset(Map<Column, Value> values) {

}
