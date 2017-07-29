import slaktor.*

data class DataRequest(val recordCount: Int, val sender: Actor)

data class DataRecord(val data: String)

object StartMsg

class Extractor : AbstractActor() {

    var lastRequestedRecordCount = 0

    val buffer = ArrayList<DataRecord>()

    override fun processMessage(message: Any) {
        if (message is DataRequest) {
            lastRequestedRecordCount = message.recordCount
            for (i in 1..message.recordCount) {
                message.sender.inbox.addMessages(buffer)
                buffer.clear()
                message.sender.inbox.addMessage(DataRecord("$i"))
                Thread.sleep(5)
            }
        }
    }

    override fun performIdleTask() {
        println("$this is idle")
        if (lastRequestedRecordCount != 0) {
            for (i in 1..100) {
                buffer.add(DataRecord("$i"))
            }
        }
    }

    override fun initialize() {}
    override fun prepareToDie() {}
}

class Transformer : AbstractActor() {

    var extractorAddr = Slaktor.spawn(Extractor::class.java)

    var buffer = ArrayList<DataRecord>()

    var destination: Actor? = null

    fun flushBuffer() {
        destination?.inbox?.addMessages(buffer)
        buffer.clear()
    }

    override fun processMessage(message: Any) {
        when (message) {
            is DataRecord -> {
                buffer.add(message)
                if (buffer.size >= 500) {
                    flushBuffer()
                }
            }
            is DataRequest -> {
                destination = message.sender
                extractorAddr?.let {
                    println("sending data request to $it")
                    Slaktor.sendTo(it, DataRequest(message.recordCount, this))
                }
            }
        }
    }

    private var timesBufferNotEmpty = 0

    override fun performIdleTask() {
        println("$this is idle")

        if (buffer == null) {
            println("it is null.")
        }
        if (buffer.isNotEmpty()) {
            timesBufferNotEmpty++
            if (timesBufferNotEmpty > 2) {
                println("!!!!!!! flushing buffer !!!!!!!")
                flushBuffer()
                timesBufferNotEmpty = 0
            }
        }
    }

    override fun initialize() {}
    override fun prepareToDie() {}

}

class Loader : AbstractActor() {

    val transformerAddr = Slaktor.spawn(Transformer::class.java)

    private fun askForData() {
        if (transformerAddr == null) return
        Slaktor.sendTo(transformerAddr, DataRequest(5000, this))
    }

    private var recordsLoaded = 0

    override fun processMessage(message: Any) {
        when (message) {
            is DataRecord -> {
                recordsLoaded++
                println("Loaded $message, number $recordsLoaded")
                if (recordsLoaded == 5000) {
                    println("asking for more data")
                    askForData()
                    recordsLoaded = 0
                }
            }
            is StartMsg -> {
                println("got START MSG!!!")
                askForData()
            }
        }
    }

    override fun performIdleTask() {
        println("$this is idle")
    }

    override fun initialize() {}
    override fun prepareToDie() {}

}

fun main(args: Array<String>) {

    Slaktor.register(Extractor::class.java, ActorConfig(), {
        println("Creating extractor")
        Extractor()
    })

    Slaktor.register(Transformer::class.java, ActorConfig(), {
        println("Creating transformer")
        Transformer()
    })

    Slaktor.register(Loader::class.java, ActorConfig(), {
        println("Creating loader")
        Loader()
    })

    val extractorAddr = Slaktor.spawn(Extractor::class.java)
    val transformerAddr = Slaktor.spawn(Transformer::class.java)
    val loaderAddr = Slaktor.spawn(Loader::class.java, StartMsg)

}
