package scalikejdbc.streams.commands

sealed trait ConsumingCommand[+R]

object ConsumingCommand {

  /**
   * Continue the consuming.
   */
  case class Continue[+R](newValue: R) extends ConsumingCommand[R]

  /**
   * Break the consuming.
   */
  case class Break[+R](newValue: R) extends ConsumingCommand[R]
}
