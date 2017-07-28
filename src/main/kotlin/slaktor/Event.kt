package slaktor

interface Event<T> {

    fun addHandler(handler: (T) -> Unit)

    fun removeHandler(handler: (T) -> Unit)

}

class StandardEvent<T> : Event<T> {

    var isEnabled = true

    private val handlers = ArrayList<(T) -> Unit>()

    override fun addHandler(handler: (T) -> Unit) {
        handlers.add(handler)
    }

    override fun removeHandler(handler: (T) -> Unit) {
        handlers.add(handler)
    }

    fun fireWith(thing: T) {
        if (!isEnabled) return
        for (handler in handlers) {
            handler.invoke(thing)
        }
    }

    fun fireWith(thingProvider: () -> T) {
        if (!isEnabled) return
        for (handler in handlers) {
            val thing = thingProvider.invoke()
            handler.invoke(thing)
        }
    }

}