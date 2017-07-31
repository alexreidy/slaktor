import slaktor.*

/*
// Strange Kotlin bug:
class Bar {
    var busy = false

    inline fun ifNotBusyPerform(action: (complete: () -> Unit) -> Unit) {
        if (busy) return
        action.invoke {
            busy = false // No crash if this line is commented out. (WTF)
            println("complete callback executing")
        }
    }

    fun ifNotBusySayHello() {
        ifNotBusyPerform { complete ->
            println("Hello!")
            complete()
        }
    }

    val myLock = Object()

    fun start() {
        // No crash if this sync block is commented out.
        // And no crash if it's replaced with another thread block.
        synchronized(myLock) {
            thread {
                // No crash if the following code is not in this thread block.
                // (Comment out the thread braces)
                ifNotBusyPerform {
                    ifNotBusySayHello() // No crash if this line is commented out.
                }
            }
        }
    }
}*/

class Extractor : AbstractActor() {
    override fun processMessage(message: Any) {
    }

    override fun performIdleTask() {
    }

    override fun initialize() {
    }

    override fun prepareToDie() {
    }
}

class Transformer : AbstractActor() {
    override fun processMessage(message: Any) {
    }

    override fun performIdleTask() {
    }

    override fun initialize() {
    }

    override fun prepareToDie() {
    }
}

class Loader : AbstractActor() {

    enum class Status { READY, BUSY }

    data class StatusRequest(val returnAddress: ActorAddress)

    data class StatusMsg(val sender: ActorAddress, val status: Status)

    private var status = Status.READY

    override fun processMessage(message: Any) {
        when (message) {
            is StatusRequest -> {
                println("Got status request.")
                Slaktor.send(StatusMsg(this.address, this.status), message.returnAddress)
            }
        }
    }

    override fun performIdleTask() {
    }

    override fun initialize() {
    }

    override fun prepareToDie() {
    }
}

class Director : AbstractActor() {

    private var extractor: ActorAddress? = null

    private var transformer: ActorAddress? = null

    override fun initialize() {
        extractor = Slaktor.spawn(Extractor::class.java)
        transformer = Slaktor.spawn(Transformer::class.java)
        for (i in 1..3) {
            Slaktor.spawn(Loader::class.java)
        }

        println("asking loaders for status")
        Slaktor.broadcastToInstancesOf(Loader::class.java,
                Loader.StatusRequest(returnAddress = this.address))
    }

    override fun processMessage(message: Any) {
        when (message) {
            is Loader.StatusMsg -> {
                println("${message.sender} is ${message.status}")
            }
        }
    }

    override fun performIdleTask() {
    }



    override fun prepareToDie() {
    }
}

fun main(args: Array<String>) {
    Slaktor.register(Director::class.java) { Director() }
    Slaktor.register(Extractor::class.java) { Extractor() }
    Slaktor.register(Transformer::class.java) { Transformer() }
    Slaktor.register(Loader::class.java) { Loader() }

    Slaktor.spawn(Director::class.java)
}
