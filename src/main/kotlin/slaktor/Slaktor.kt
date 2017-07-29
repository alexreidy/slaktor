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
        var instanceManager: CollectionManager<Actor>)

object Slaktor {

    private val infoForActorType = ConcurrentHashMap<Class<*>, ActorTypeInfo>()

    private val actorsByAddress = ConcurrentHashMap<ActorAddress, Actor>()

    private val actorsInGroup = ConcurrentHashMap<ActorGroup, MutableSet<Actor>>()

    fun register(actorType: Class<*>, actorConfig: ActorConfig, factory: () -> Actor) {
        val instanceManager = CollectionManager<Actor>(HashSet<Actor>())
        infoForActorType[actorType] = ActorTypeInfo(actorType, actorConfig, factory, instanceManager)
    }

    /**
     * If the Actor type is registered, spawns an instance and returns the address.
     */
    // todo: should probably have a way to pass a config or initial message.
    fun spawn(actorType: Class<*>): ActorAddress? {
        val actorTypeInfo = infoForActorType[actorType]
        if (actorTypeInfo == null) return null
        val actor = actorTypeInfo.factory.invoke()
        actorsByAddress[actor.address] = actor
        actorTypeInfo.instanceManager.inbox.addMessage(
                CollectionManager.Messages.Add(listOf(actor)))
        return actor.address
    }

    fun sendTo(actorAddress: ActorAddress, message: Any) {
        actorsByAddress[actorAddress]?.inbox?.addMessage(message)
    }

    fun sendTo(actorAddress: ActorAddress, messages: Iterable<Any>) {
        actorsByAddress[actorAddress]?.inbox?.addMessages(messages)
    }

    fun broadcastToInstancesOf(actorType: Class<*>, message: Any) {
        infoForActorType[actorType]?.instanceManager?.inbox?.addMessage(
                CollectionManager.Messages.ForEach { actor ->
                    (actor as Actor).inbox.addMessage(message)
                })
    }

    fun broadcastToInstancesOf(actorType: Class<*>, messages: Iterable<Any>) {
        infoForActorType[actorType]?.instanceManager?.inbox?.addMessage(
                CollectionManager.Messages.ForEach { actor ->
                    (actor as Actor).inbox.addMessages(messages)
                })
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