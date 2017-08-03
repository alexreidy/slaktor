package slaktor

class QueuedOpConcurrentCollection<T>(private val backingCollection: MutableCollection<T>) {

    private val collectionManager = CollectionManager(backingCollection)

    init {
        collectionManager.start()
    }

    /**
     * Adds the things to the backing collection.
     * @param then Called after the things are added.
     */
    fun addAsync(things: Iterable<T>, then: () -> Unit = {}) {
        collectionManager.inbox.addMessage(
                CollectionManager.Messages.Add(things,
                        finishedCallback = then))
    }

    /**
     * Removes the things from the backing collection.
     * @param then Called after the things are removed.
     */
    fun removeAsync(things: Iterable<T>, then: () -> Unit = {}) {
        collectionManager.inbox.addMessage(
                CollectionManager.Messages.Remove(things,
                        finishedCallback = then))
    }

    /**
     * Calls the given action for each element, passing the element to the action.
     * @param then Called after performing the forEach.
     */
    fun forEachAsync(action: (T) -> Unit, then: () -> Unit = {}) {
        collectionManager.inbox.addMessage(
                CollectionManager.Messages.ForEach({ action(it as T) },
                        finishedCallback = then))
    }

    fun dispose() {
        collectionManager.shutdown()
    }
}

private class CollectionManager<T>(private val things: MutableCollection<T>) : AbstractActor() {

    class Messages {
        /**
         * Adds the things.
         */
        data class Add(
                val things: Iterable<*>,
                val finishedCallback: () -> Unit = {})

        /**
         * Removes the things.
         */
        data class Remove(
                val things: Iterable<*>,
                val finishedCallback: () -> Unit = {})

        /**
         * Calls the given function for each element, passing the element.
         */
        data class ForEach(
                val action: (Any?) -> Unit,
                val finishedCallback: () -> Unit = {})
    }

    override fun performIdleTask() {}

    override fun prepareToDie() {}

    override fun processMessage(message: Any) {
        when (message) {
            is Messages.ForEach -> {
                things.forEach { message.action.invoke(it) }
                message.finishedCallback.invoke()
            }
            is Messages.Add -> {
                for (thingToAdd in message.things) {
                    things.add(thingToAdd as T)
                }
                message.finishedCallback.invoke()
            }
            is Messages.Remove -> {
                for (thingToRemove in message.things) {
                    things.remove(thingToRemove)
                }
                message.finishedCallback.invoke()
            }
        }
    }

}