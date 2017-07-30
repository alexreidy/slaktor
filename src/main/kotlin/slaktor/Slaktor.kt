package slaktor

import java.util.concurrent.ConcurrentHashMap

data class ActorAddress(val address: String)

private data class ActorTypeInfo(
        var type: Class<*>,
        var factory: () -> Actor,
        var instances: QueuedOpConcurrentCollection<Actor>)

object Slaktor {

    private val infoForActorType = ConcurrentHashMap<Class<*>, ActorTypeInfo>()

    private val actorsByAddress = ConcurrentHashMap<ActorAddress, Actor>()

    fun register(actorType: Class<*>, factory: () -> Actor) {
        val instances = QueuedOpConcurrentCollection(HashSet<Actor>())
        infoForActorType[actorType] = ActorTypeInfo(actorType, factory, instances)
    }

    /**
     * If the Actor type is registered, spawns an instance and returns the address.
     * @param initMessage If present, this is sent to the actor before the actor is
     * added to the public pool where other messages can reach it.
     */
    fun spawn(actorType: Class<*>, initMessage: Any? = null): ActorAddress? {
        val actorTypeInfo = infoForActorType[actorType]
        if (actorTypeInfo == null) return null

        val actor = actorTypeInfo.factory.invoke()
        actor.start()
        if (initMessage != null) actor.inbox.addMessage(initMessage)

        actorsByAddress[actor.address] = actor
        actorTypeInfo.instances.addAsync(listOf(actor))

        return actor.address
    }

    fun kill(actorAddress: ActorAddress) {
        val actor = actorsByAddress[actorAddress] ?: return
        actorsByAddress.remove(actorAddress)
        val actorTypeInfo = infoForActorType[actor.javaClass] ?: return
        actorTypeInfo.instances.removeAsync(listOf(actor))
        actor.shutdown()
    }

    fun killAllInstancesOf(actorType: Class<*>) {
        val actorTypeInfo = infoForActorType[actorType] ?: return
        val deadActors = ArrayList<Actor>()
        actorTypeInfo.instances.forEachAsync {
            it.shutdown()
            actorsByAddress.remove(it.address)
            deadActors.add(it)
        }
        actorTypeInfo.instances.removeAsync(deadActors)
    }

    fun send(message: Any, address: ActorAddress) {
        actorsByAddress[address]?.inbox?.addMessage(message)
    }

    fun sendAll(messages: Iterable<Any>, address: ActorAddress) {
        actorsByAddress[address]?.inbox?.addMessages(messages)
    }

    fun broadcastToInstancesOf(actorType: Class<*>, message: Any) {
        infoForActorType[actorType]?.instances?.forEachAsync { actor ->
            actor.inbox.addMessage(message)
        }
    }

    fun broadcastAllToInstancesOf(actorType: Class<*>, messages: Iterable<Any>) {
        infoForActorType[actorType]?.instances?.forEachAsync { actor ->
            actor.inbox.addMessages(messages)
        }
    }

}