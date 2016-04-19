package sparkSql

import scala.collection.mutable.Queue

/**
  * Created by iceke on 16/4/19.
  */
object SomeTest {
  def main(args: Array[String]): Unit ={
    val downloadQueue = Queue[Int]()
    downloadQueue.enqueue(1)
    downloadQueue.enqueue(3)
    downloadQueue.enqueue(5)
    downloadQueue.enqueue(2)
    val x = downloadQueue.dequeue
    val y = downloadQueue.dequeue
  }

}
