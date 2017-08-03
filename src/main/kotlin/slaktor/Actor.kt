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

    /**
     * Performs the action if the primary actor thread is available. The action will receive
     * a `complete` callback that should be called when the action is finished.
     * The thread will be unavailable to others until `complete()` is called.
     * (This exists in case the action finishes on another thread.)
     */
    private inline fun ifActorThreadIsAvailable(action: (freeActorThread: () -> Unit) -> Unit) {
        val allowedToRun = actorThreadIsBusy.compareAndSet(false, true)
        if (!allowedToRun) return
        action.invoke {
            actorThreadIsBusy.set(false)
        }
    }

    private inline fun processInboxIfActorThreadIsAvailable() {
        ifActorThreadIsAvailable { freeActorThread ->
            threadPool.execute {
                var message: Any? = _inbox.nextMessage
                while (alive && message != null) {
                    processMessage(message)
                    message = _inbox.nextMessage
                }
                freeActorThread()
            }
        }
    }

    constructor() {
        _inbox.messagesAddedEvent.addHandler {
            if (!alive) return@addHandler
            processInboxIfActorThreadIsAvailable()
        }
    }

    /**
     * Called for each message received while the actor is alive.
     */
    protected abstract fun processMessage(message: Any)

    protected abstract fun performIdleTask()

    /**
     * Called when alive and `shutdown()` is called.
     * This is where you get your affairs in order.
     */
    protected abstract fun prepareToDie()

    protected var millisToSleepBetweenIdleTaskAttempts = 1000L

    override final fun start() {
        synchronized(alive) {
            if (alive) return
            alive = true
            thread {
                while (alive) {
                    ifActorThreadIsAvailable { freeActorThread ->
                        performIdleTask()
                        freeActorThread()
                        processInboxIfActorThreadIsAvailable()
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