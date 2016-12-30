package scalikejdbc.streams

import java.sql.ResultSet

import scalikejdbc._

abstract class StreamingInvoker[A, E <: WithExtractor] {
  protected[this] def streamingSql: StreamingSQL[A, E]

  def results()(implicit session: DBSession): CloseableIterator[A] = {
    val streamingSession = streamingSql.setSessionAttributes(session)
    val sql = streamingSql.underlying
    val executor = streamingSession.toStatementExecutor(sql.statement, sql.rawParameters)
    val proxy = new DBConnectionAttributesWiredResultSet(executor.executeQuery(), streamingSession.connectionAttributes)
    new StreamingInvoker.ExtractedResultIteratorImpl[A](executor, proxy, true)(sql.extractor)
  }
}

object StreamingInvoker {

  private class ExtractedResultIteratorImpl[+A](
      executor: StatementExecutor,
      rs: ResultSet,
      autoClose: Boolean
  )(extract: WrappedResultSet => A) extends ExtractedResultIterator[A](rs, autoClose)(extract) {
    private[this] var closed = false

    override def close(): Unit = if (!closed) {
      executor.close()
      closed = true
    }
  }
}
