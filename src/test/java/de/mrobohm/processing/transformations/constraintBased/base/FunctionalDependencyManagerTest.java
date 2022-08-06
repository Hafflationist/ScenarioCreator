package de.mrobohm.processing.transformations.constraintBased.base;

import de.mrobohm.data.identification.IdSimple;
import de.mrobohm.data.table.FunctionalDependency;
import de.mrobohm.utils.SSet;
import org.junit.jupiter.api.Test;

class FunctionalDependencyManagerTest {


    private FunctionalDependency of(int a, int b) {
        return new FunctionalDependency(SSet.of(new IdSimple(a)), SSet.of(new IdSimple(b)));
    }

    @Test
    void attributeClosure() {
        // --- Arrange
        var fdSet = SSet.of(
                of(1,2),
                of(2,3),
                of(3,1),
                of(4,5)
        );
        var attr = new IdSimple(1);

        // --- Act
        //var closureImp = FunctionalDependencyCalculator.attributeClosureImperative(SSet.of(attr), fdSet);
        var closureFun = FunctionalDependencyManager.attributeClosure(SSet.of(attr), fdSet);

        // --- Assert
//        Assertions.assertEquals(closureFun, closureImp);
    }
}