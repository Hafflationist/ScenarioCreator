package correspondence

trait ExistsExpression[T] {}
case class ExistsDisjunction[T](parts: Set[ExistsExpression[T]]) extends ExistsExpression[T] {
    override def toString: String = parts.map(ee => "(" ++ ee.toString ++ ")").mkString(" oder ")
}
case class ExistsConjunction[T](parts: Set[ExistsExpression[T]]) extends ExistsExpression[T] {
    override def toString: String = parts.map(ee => "(" ++ ee.toString ++ ")").mkString(" und ")
}
case class ExistsTerminal[T](part: T, show: T => String) extends ExistsExpression[T] {
    override def toString: String = show(part)

    override def equals(obj: Any): Boolean = {
        obj match {
            case terminal: ExistsTerminal[T] => terminal.part == part
            case _ => false
        }
    }
}
case class ExistsNowhere[T]() extends ExistsExpression[T]
