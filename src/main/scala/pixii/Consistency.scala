package pixii

sealed trait Consistency {
  def orElse(c: Consistency): Consistency
  def isConsistentRead: Boolean
}

case object UnspecifiedConsistency extends Consistency {
  override def orElse(c: Consistency) = c
  override def isConsistentRead = false
}

case object EventuallyConsistent extends Consistency {
  override def orElse(c: Consistency) = this
  override def isConsistentRead = false
}

case object ConsistentRead extends Consistency {
  override def orElse(c: Consistency) = this
  override def isConsistentRead = true
}
