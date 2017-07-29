package slaktor

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

interface Actor {

    val address: ActorAddress

    val inbox: Inbox<Any>

    fun shutdown()

}

private val executor = Executors.newCachedThreadPool()

abstract class AbstractActor : Actor {

    override val address = ActorAddress(this.toString())

    private val _inbox = StandardInbox<Any>()

    override val inbox: Inbox<Any> = _inbox

    private val actorThreadIsBusy = AtomicBoolean()

    private inline fun ifActorThreadIsAvailable(action: (complete: () -> Unit) -> Unit) {
        val allowedToRun = actorThreadIsBusy.compareAndSet(false, true)
        if (!allowedToRun) return
        action.invoke {
            actorThreadIsBusy.set(false)
        }
    }

    constructor() {
        _inbox.messagesAddedEvent.addHandler {
            ifActorThreadIsAvailable { complete ->
                executor.execute {
                    var message: Any? = _inbox.nextMessage
                    while (message != null) {
                        processMessage(message)
                        message = _inbox.nextMessage
                    }
                    complete()
                }
            }
        }

        thread {
            Thread.sleep(1000) //todo
            while (true) {
                ifActorThreadIsAvailable { complete ->
                    performIdleTask()
                    complete()
                }
                Thread.sleep(1000)
            }
        }
    }

    protected abstract fun processMessage(message: Any)

    protected abstract fun performIdleTask()

    protected abstract fun prepareToDie()

    override fun shutdown() {
        prepareToDie()
    }

}