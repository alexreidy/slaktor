package slaktor

class QueuedOpConcurrentCollection<T>(private val backingCollection: MutableCollection<T>) {

    private val collectionManager = CollectionManager(backingCollection)

    fun addAsync(things: Iterable<T>) {
        collectionManager.inbox.addMessage(
                CollectionManager.Messages.Add(things))
    }

    fun removeAsync(things: Iterable<T>) {
        collectionManager.inbox.addMessage(
                CollectionManager.Messages.Remove(things))
    }

    fun forEachAsync(action: (T) -> Unit) {
        collectionManager.inbox.addMessage(
                CollectionManager.Messages.ForEach { action(it as T) })
    }
}

private class CollectionManager<T>(private val things: MutableCollection<T>) : AbstractActor() {

    class Messages {
        /**
         * Adds the things.
         */
        data class Add(val things: Iterable<*>)

        /**
         * Removes the things.
         */
        data class Remove(val things: Iterable<*>)

        /**
         * Calls the given function for each element, passing the element.
         */
        data class ForEach(val action: (Any?) -> Unit)
    }

    override fun performIdleTask() {}

    override fun initialize() {}

    override fun prepareToDie() {}

    override fun processMessage(message: Any) {
        when (message) {
            is Messages.ForEach -> {
                things.forEach { message.action.invoke(it) }
            }
            is Messages.Add -> {
                for (thingToAdd in message.things) {
                    things.add(thingToAdd as T)
                }
            }
            is Messages.Remove -> {
                for (thingToRemove in message.things) {
                    things.remove(thingToRemove)
                }
            }
        }
    }

}