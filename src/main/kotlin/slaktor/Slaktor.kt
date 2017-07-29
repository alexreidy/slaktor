package slaktor

import java.util.concurrent.ConcurrentHashMap

data class ActorType(val id: String)

data class ActorAddress(val address: String)

data class ActorGroup(val id: String)

class ActorConfig

private data class ActorTypeInfo(
        var type: Class<*>,
        var config: ActorConfig,
        var factory: () -> Actor,
        var instances: QueuedOpConcurrentCollection<Actor>)

object Slaktor {

    private val infoForActorType = ConcurrentHashMap<Class<*>, ActorTypeInfo>()

    private val actorsByAddress = ConcurrentHashMap<ActorAddress, Actor>()

    private val actorsInGroup = ConcurrentHashMap<ActorGroup, MutableSet<Actor>>()

    fun register(actorType: Class<*>, actorConfig: ActorConfig, factory: () -> Actor) {
        val instances = QueuedOpConcurrentCollection(HashSet<Actor>())
        infoForActorType[actorType] = ActorTypeInfo(actorType, actorConfig, factory, instances)
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
        if (initMessage != null) actor.inbox.addMessage(initMessage)

        actorsByAddress[actor.address] = actor
        actorTypeInfo.instances.addAsync(listOf(actor))

        return actor.address
    }

    fun sendTo(actorAddress: ActorAddress, message: Any) {
        actorsByAddress[actorAddress]?.inbox?.addMessage(message)
    }

    fun sendTo(actorAddress: ActorAddress, messages: Iterable<Any>) {
        actorsByAddress[actorAddress]?.inbox?.addMessages(messages)
    }

    fun broadcastToInstancesOf(actorType: Class<*>, message: Any) {
        infoForActorType[actorType]?.instances?.forEachAsync { actor ->
            actor.inbox.addMessage(message)
        }
    }

    fun broadcastToInstancesOf(actorType: Class<*>, messages: Iterable<Any>) {
        infoForActorType[actorType]?.instances?.forEachAsync { actor ->
            actor.inbox.addMessages(messages)
        }
    }

    fun broadcastToGroup(actorGroup: ActorGroup, message: Any) {
        actorsInGroup[actorGroup]?.forEach {
            it.inbox.addMessage(message)
        }
    }

    fun broadcastToGroup(actorGroup: ActorGroup, messages: Iterable<Any>) {
        actorsInGroup[actorGroup]?.forEach {
            it.inbox.addMessages(messages)
        }
    }

}