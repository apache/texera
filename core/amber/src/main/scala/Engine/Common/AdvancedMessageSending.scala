package Engine.Common

import Engine.Common.AmberMessage.ControlMessage.QueryState
import akka.actor.{ActorRef, Scheduler}
import akka.event.LoggingAdapter
import akka.pattern._
import akka.util.Timeout

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scala.util.control.Breaks
import scala.concurrent.duration._

object AdvancedMessageSending {

  def nonBlockingAskWithRetry(receiver: ActorRef, message: Any, maxAttempts: Int, attempt: Int)(implicit timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter): Future[Any] = {
    val future = (receiver ? message) recover {
      case e: AskTimeoutException =>
        if (attempt > maxAttempts) log.error("failed to send message "+message+" to "+receiver)
        else nonBlockingAskWithRetry(receiver, message, maxAttempts, attempt + 1)
    }
    future
  }

  def nonBlockingAskWithRetry(receiver: ActorRef, message: Any, maxAttempts: Int, attempt: Int, callback: Any => Unit)(implicit timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter): Unit = {
    (receiver ? message) onComplete {
      case Success(value) => callback(value)
      case Failure(exception) =>
        if (attempt > maxAttempts) log.error("failed to send message "+message+" to "+receiver)
        else nonBlockingAskWithRetry(receiver, message, maxAttempts, attempt + 1,callback)
    }
  }

//  def nonBlockingAskWithCondition(receiver: ActorRef, message: Any, cond:Any => Boolean, delay:FiniteDuration = 10.seconds)(implicit timeout:Timeout, ec:ExecutionContext, scheduler:Scheduler): Future[Any] ={
//    val future = (receiver ? message) recover {
//      case e: AskTimeoutException =>
//        (receiver ? QueryState) onComplete{
//          case Success(value) =>
//            if(cond(value)){
//              after(delay,scheduler){nonBlockingAskWithCondition(receiver,message,cond,delay)}
//            }
//          case Failure(_) =>
//            after(delay,scheduler){nonBlockingAskWithCondition(receiver,message,cond,delay)}
//        }
//    }
//    future
//  }

  //this is blocking the actor, be careful!
  def blockingAskWithRetry(receiver: ActorRef, message: Any, maxAttempts: Int)(implicit timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter):Any ={
    var res:Any = null
    Breaks.breakable{
      var i = 0
      while(i < maxAttempts){
        Try{
          res = Await.result(receiver ? message,timeout.duration)
          Breaks.break()
        }
        i += 1
      }
      log.error("failed to send message "+message+" to "+receiver)
    }
    res
  }

  //this is blocking the actor, be careful!
  def blockingAskWithRetry(receiver: ActorRef, message: Any, maxAttempts: Int, callback: Any => Unit)(implicit timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter):Unit ={
    var res:Any = null
    Breaks.breakable{
      var i = 0
      while(i < maxAttempts){
        Try{
          res = Await.result(receiver ? message,timeout.duration)
          Breaks.break()
        }
        i += 1
      }
      log.error("failed to send message "+message+" to "+receiver)
    }
    callback(res)
  }

}
