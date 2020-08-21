package Engine.Operators.Filter

import Engine.Common.AmberException.AmberException

import scala.language.postfixOps
import Ordering.Implicits._
import com.github.nscala_time.time.Imports._


object FilterType extends Enumeration with Serializable {


  object AmberDateTime{
    def parse(str:String): AmberDateTime = new AmberDateTime(DateTime.parse(str))
  }

  class AmberDateTime(val s: DateTime) extends Ordering[DateTime] {
    override def compare(x: DateTime, y: DateTime): Int = x.compare(y)
  }



  case class Val[T:Ordering](validate: (T,T) => Boolean) extends super.Val with Serializable
  def Equal[T:Ordering]: Val[T] = Val[T]((x, y) => x==y)
  def Greater[T:Ordering]: Val[T] = Val[T]((x, y) => x>y)
  def GreaterOrEqual[T:Ordering]: Val[T] = Val[T]((x, y) => x>=y)
  def Less[T:Ordering]: Val[T] = Val[T]((x, y) => x<y)
  def LessOrEqual[T:Ordering]: Val[T] = Val[T]((x, y) => x<=y)
  def NotEqual[T:Ordering]: Val[T] = Val[T]((x, y) => x!=y)

  def getType[T:Ordering](string: String):Val[T] = {
    string match{
      case "Equal" => Equal
      case "Greater" => Greater
      case "GreaterOrEqual" => GreaterOrEqual
      case "Less" => Less
      case "LessOrEqual" => LessOrEqual
      case "NotEqual" => NotEqual
      case _ => throw new AmberException("filter type doesn't match with any exist type")
    }
  }
}






