package id

import org.junit.jupiter.api.{Assertions, Test}
import scenarioCreator.data.identification._

class ExtractorTest {
    @Test
    private[id] def removeMerges(): Unit = {
        // --- Arrange
        val rootId = new IdSimple(0)
        val idWithMerge = new IdPart(
            new IdMerge(
                new IdPart(
                    new IdMerge(
                        new IdSimple(0),
                        new IdSimple(1),
                        MergeOrSplitType.Other
                    ),
                    1,
                    MergeOrSplitType.Other
                ),
                new IdPart(
                    new IdMerge(
                        new IdSimple(2),
                        new IdSimple(3),
                        MergeOrSplitType.Other
                    ),
                    1,
                    MergeOrSplitType.Other
                ),
                MergeOrSplitType.Other
            ),
            1,
            MergeOrSplitType.Other
        )
        val idWithoutMerge: Id = new IdPart(
            new IdPart(
                new IdSimple(0),
                1,
                MergeOrSplitType.Other
            ),
            1,
            MergeOrSplitType.Other
        )

        // --- Act
        val resultId = Extractor.removeMerges(idWithMerge, rootId)

        // --- Assert
        Assertions.assertEquals(idWithoutMerge, resultId)
    }
}
