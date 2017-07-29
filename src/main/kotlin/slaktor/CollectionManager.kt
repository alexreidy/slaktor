package slaktor

// Todo: wrap in traditional class for type safety?

class CollectionManager<T>(private val things: MutableCollection<T>) : AbstractActor() {

    class Messages {
        /**
         * Adds the things.
         */
        data class Add(val things: Iterable<Any>)

        /**
         * Removes the things.
         */
        data class Remove(val things: Iterable<Any>)

        /**
         * Calls the given function for each element, passing the element.
         */
        data class ForEach(val action: (Any) -> Unit)
    }

    override fun performIdleTask() {}

    override fun prepareToDie() {}

    override fun processMessage(message: Any) {
        println("processing message: $message")
        when (message) {
            is Messages.ForEach -> {
                things.forEach { message.action.invoke(it as Any) }
            }
            is Messages.Add -> {
                for (thingToAdd in message.things) {
                    things.add(thingToAdd as T)
                }
            }
            is Messages.Remove -> {
                for (thingToRemove in message.things) {
                    things.remove(thingToRemove as T)
                }
            }
        }
    }

}