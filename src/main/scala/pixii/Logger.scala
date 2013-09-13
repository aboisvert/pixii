package pixii

trait Logger {
  def warn(message: String): Unit
  def error(message: String, t: Throwable): Unit
}

trait ConsoleLogger extends Logger {
  override def warn(message: String) {
    Console.println("[WARN]  " + message)
  }

  override def error(message: String, t: Throwable) {
    Console.println("[ERROR] " + message)
    t.printStackTrace()
  }
}