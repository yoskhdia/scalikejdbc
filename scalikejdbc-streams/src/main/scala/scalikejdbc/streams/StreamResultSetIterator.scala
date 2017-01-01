package scalikejdbc.streams

import java.io.Closeable
import java.sql.ResultSet

import scalikejdbc.{ ResultSetCursor, WrappedResultSet }

/**
 * An iterator which handles JDBC ResultSet in the fashion of Reactive Streams.
 */
private[streams] class StreamResultSetIterator[+A](
    rs: ResultSet,
    autoClose: Boolean
)(extract: WrappedResultSet => A) extends BufferedIterator[A] with Closeable { self =>

  private[this] var state = 0 // 0: no data, 1: cached, 2: finished
  private[this] var preFetchedNextValue: A = null.asInstanceOf[A]

  protected[this] final def markedAsFinishedAndReturnNullValue(): A = {
    state = 2
    null.asInstanceOf[A]
  }

  def head: A = {
    update()
    if (state == 1) preFetchedNextValue
    else throw new NoSuchElementException("head on empty iterator")
  }

  private[this] def update(): Unit = {
    if (state == 0) {
      preFetchedNextValue = fetchNext()
      if (state == 0) state = 1
    }
  }

  def hasNext: Boolean = {
    update()
    state == 1
  }

  def next(): A = {
    update()
    if (state == 1) {
      state = 0
      preFetchedNextValue
    } else throw new NoSuchElementException("next on empty iterator")
  }

  private[this] val cursor: ResultSetCursor = new ResultSetCursor(0)

  protected def fetchNext(): A = {
    if (rs.next()) {
      cursor.position += 1
      val res = extract(WrappedResultSet(rs, cursor, cursor.position))
      res
    } else {
      if (autoClose) close()
      markedAsFinishedAndReturnNullValue()
    }
  }

  override def close(): Unit = {
    self.close()
  }

}