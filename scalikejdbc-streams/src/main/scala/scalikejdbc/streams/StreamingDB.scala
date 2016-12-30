package scalikejdbc.streams

import org.reactivestreams.Subscriber
import scalikejdbc._

class StreamingDB(
    val executor: StreamingDB.Executor,
    val name: Any,
    val connectionPoolContext: DB.CPContext,
    val settings: SettingsProvider,
    val bufferNext: Boolean = true
) extends StreamingDB.DatabaseContainer {
  self =>

  def stream[A, E <: WithExtractor](sql: StreamingSQL[A, E]): DatabasePublisher[A] = {
    val action = createAction(sql)
    createPublisher(action)
  }

  protected[this] def createAction[A, E <: WithExtractor](sql: StreamingSQL[A, E]): StreamingAction[A, E] = StreamingDB.createAction(sql)

  protected[this] def createPublisher[A, E <: WithExtractor](action: StreamingAction[A, E]): DatabasePublisher[A] = StreamingDB.createPublisher(action, self)

}

object StreamingDB extends StreamingDatabaseComponent {
  override type Executor = AsyncExecutor
  override type StreamingActionContext = StreamingContext

  override def createContext(database: DatabaseContainer, subscriber: Subscriber[_]): StreamingActionContext = new StreamingContext(subscriber, database, database.bufferNext)

  def apply(dbName: Any, executor: Executor)(implicit
    context: DB.CPContext = DB.NoCPContext,
    settingsProvider: SettingsProvider = SettingsProvider.default): StreamingDB = {
    new StreamingDB(executor, dbName, context, settingsProvider)
  }
}
