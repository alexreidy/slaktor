package slaktor

import java.util.concurrent.ConcurrentLinkedQueue

interface Inbox<T> {

    fun addMessage(message: T)

    fun addMessages(messages: Iterable<T>)

}

interface ConsumableInbox<T> : Inbox<T> {

    val messagesAddedEvent: Event<Unit>

    val nextMessage: T?

}

class StandardInbox<T> : ConsumableInbox<T> {

    private val messageQueue = ConcurrentLinkedQueue<T>()

    private val _messagesAddedEvent = StandardEvent<Unit>()

    override val messagesAddedEvent: Event<Unit> = _messagesAddedEvent

    override fun addMessage(message: T) {
        messageQueue.add(message)
        _messagesAddedEvent.fireWith(Unit)
    }

    override fun addMessages(messages: Iterable<T>) {
        messageQueue.addAll(messages)
        _messagesAddedEvent.fireWith(Unit)
    }

    override val nextMessage: T?
        get() {
            return messageQueue.poll()
        }
}