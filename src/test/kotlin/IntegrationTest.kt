import slaktor.*
import kotlin.concurrent.thread

/*
// Strange Kotlin bug:
class Foo {
    var bar = false

    /*
    inline fun ifNotBusyPerform(action: (complete: () -> Unit) -> Unit) {
        action.invoke {
            bar = false // No crash if this line is commented out. (WTF)
            println("complete callback executing")
        }
    }*/

    // No crash if this is made inline
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
                    /* No crash if the following replaces the above method call
                    (as expected - there's no crash if the above method is made inline).
                    ifNotBusyPerform { complete ->
                        println("Hello!")
                        complete()
                    }*/
                }
            }
        }
    }

    inline fun ifNotBusyPerform(action: (complete: () -> Unit) -> Unit) {
        action.invoke {
            bar = false // No crash if this line is commented out. (WTF)
            println("complete callback executing")
        }
    }
}*/

class Extractor : AbstractActor() {
    override fun processMessage(message: Any) {
    }

    override fun performIdleTask() {
    }

    override fun prepareToDie() {
    }
}

class Transformer : AbstractActor() {
    override fun processMessage(message: Any) {
    }

    override fun performIdleTask() {
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
        println("idling")
    }

    override fun prepareToDie() {
        println("going down")
    }
}

class Director : AbstractActor {

    private var extractor: ActorAddress? = null

    private var transformer: ActorAddress? = null

    constructor() {
        initialize()
    }

    fun initialize() {
        extractor = Slaktor.spawn(Extractor::class.java)
        transformer = Slaktor.spawn(Transformer::class.java)
        for (i in 1..35) {
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

    Thread.sleep(5000)
    Slaktor.killAllInstancesOf(Loader::class.java)
}
