package de.mrobohm.data.column.context;

public enum UnitOfMeasure {
    Exa (15),
    Tera(12),
    Giga(9),
    Mega(6),
    Kilo(3),
    Hecto(2),
    Deca(1),
    Pure(0),
    Deci(-1),
    Centi(-2),
    Milli(-3),
    Micro(-6),
    Nano(-9),
    Pico(-12),
    None(0);


    private final int _factorLog10;

    UnitOfMeasure(int factorLog10) {
        _factorLog10 = factorLog10;
    }

    public int getFactorLog10() {
        return _factorLog10;
    }
}
