package slaktor

import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

interface Actor {

    val address: ActorAddress

    val inbox: Inbox<Any>

    fun start()

    fun shutdown()

}

abstract class AbstractActor : Actor {

    override val address = ActorAddress(this.toString())

    private val _inbox = StandardInbox<Any>()

    override val inbox: Inbox<Any> = _inbox

    private val actorThreadIsBusy = AtomicBoolean()

    @Volatile private var alive = false

    private var initialized = false

    /**
     * Performs the action if the primary actor thread is available. The action will receive
     * a `complete` callback that should be called when the action is finished.
     * The thread will be unavailable to others until `complete()` is called.
     * (This exists in case the action finishes on another thread.)
     */
    private inline fun ifActorThreadIsAvailable(action: (complete: () -> Unit) -> Unit) {
        val allowedToRun = actorThreadIsBusy.compareAndSet(false, true)
        if (!allowedToRun) return
        action.invoke {
            actorThreadIsBusy.set(false)
        }
    }

    constructor() {
        _inbox.messagesAddedEvent.addHandler {
            if (!alive) return@addHandler
            ifActorThreadIsAvailable { complete ->
                threadPool.execute {
                    var message: Any? = _inbox.nextMessage
                    while (message != null) {
                        processMessage(message)
                        message = _inbox.nextMessage
                    }
                    complete()
                }
            }
        }
    }

    /**
     * Called for each message received while the actor is alive.
     */
    protected abstract fun processMessage(message: Any)

    protected abstract fun performIdleTask()

    protected abstract fun initialize()

    /**
     * Called when alive and `shutdown()` is called.
     */
    protected abstract fun prepareToDie()

    protected var millisToSleepBetweenIdleTaskAttempts = 1000L

    override final fun start() {
        synchronized(alive) {
            if (initialized || alive) return
            initialize()
            initialized = true
            alive = true
            thread {
                while (alive) {
                    ifActorThreadIsAvailable { complete ->
                        performIdleTask()
                        complete()
                    }
                    Thread.sleep(millisToSleepBetweenIdleTaskAttempts)
                }
            }
        }
    }

    override final fun shutdown() {
        synchronized(alive) {
            if (!alive) return
            prepareToDie()
            alive = false
        }
    }

}