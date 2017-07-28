package slaktor

import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

interface Actor {

    val inbox: Inbox<Any>

    fun shutdown()

}

private val executor = Executors.newCachedThreadPool()

abstract class AbstractActor : Actor {

    private val _inbox = StandardInbox<Any>()

    override val inbox: Inbox<Any> = _inbox

    private val processingThreadIsBusy = AtomicBoolean()

    private inline fun ifProcessingThreadIsAvailable(action: (complete: () -> Unit) -> Unit) {
        val allowedToRun = processingThreadIsBusy.compareAndSet(false, true)
        if (!allowedToRun) return
        action.invoke {
            processingThreadIsBusy.set(false)
        }
    }

    constructor() {
        _inbox.messagesAddedEvent.addHandler {
            ifProcessingThreadIsAvailable { complete ->
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
            while (true) {
                ifProcessingThreadIsAvailable { complete ->
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