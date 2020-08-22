package Engine.Common

object AmberUtils {

  def reverseMultimap[T1, T2](map: Map[T1, Set[T2]]): Map[T2, Set[T1]] =
    map.toSeq
      .flatMap { case (k, vs) => vs.map((_, k)) }
      .groupBy(_._1)
      .mapValues(_.map(_._2).toSet)

}
