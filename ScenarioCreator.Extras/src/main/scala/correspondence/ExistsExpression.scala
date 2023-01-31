package scenarioCreator.extra.correspondence

trait ExistsExpression[T] {}
case class ExistsDisjunction[T](parts: Set[ExistsExpression[T]]) extends ExistsExpression[T]
case class ExistsConjunction[T](parts: Set[ExistsExpression[T]]) extends ExistsExpression[T]
case class ExistsTerminal[T](part: T) extends ExistsExpression[T]