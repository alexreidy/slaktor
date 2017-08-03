package slaktor

interface Logger {

    fun fatal(message: String)

    fun error(message: String)

    fun warn(message: String)

    fun info(message: String)

    fun debug(message: String)

    fun trace(message: String)

}

private fun printlog(level: String, message: String) = println("Slaktor | $level | $message")

class ConsoleLogger : Logger {
    override fun fatal(message: String) = printlog("FATAL", message)
    override fun error(message: String) = printlog("ERROR", message)
    override fun warn(message: String) = printlog("WARN", message)
    override fun info(message: String) = printlog("INFO", message)
    override fun debug(message: String) = printlog("DEBUG", message)
    override fun trace(message: String) = printlog("TRACE", message)
}

class NoLogger : Logger {
    override fun fatal(message: String) {}
    override fun error(message: String) {}
    override fun warn(message: String) {}
    override fun info(message: String) {}
    override fun debug(message: String) {}
    override fun trace(message: String) {}
}