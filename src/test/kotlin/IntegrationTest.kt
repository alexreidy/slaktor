import slaktor.*

data class DataRequest(val recordCount: Int, val senderAddress: ActorAddress)

data class DataRecord(val data: String)

object StartMsg

class Extractor : AbstractActor() {

    var lastRequestedRecordCount = 0

    val buffer = ArrayList<DataRecord>()

    override fun processMessage(message: Any) {
        if (message is DataRequest) {
            lastRequestedRecordCount = message.recordCount
            for (i in 1..message.recordCount) {
                Slaktor.sendAll(buffer, message.senderAddress)
                buffer.clear()
                Slaktor.send(DataRecord("$i"), message.senderAddress)
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
    override fun prepareToDie() {
        println("$this: It's quite alright; I've grown tired of living")
    }
}

class Transformer : AbstractActor() {

    var extractorAddr = Slaktor.spawn(Extractor::class.java)

    var buffer = ArrayList<DataRecord>()

    var destinationAddr: ActorAddress? = null

    fun flushBuffer() {
        Slaktor.sendAll(buffer, destinationAddr!!)
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
                destinationAddr = message.senderAddress
                extractorAddr?.let {
                    println("sending data request to $it")
                    Slaktor.send(DataRequest(message.recordCount, this.address), it)
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
                Slaktor.kill(extractorAddr!!)
            }
        }
    }

    override fun initialize() {}
    override fun prepareToDie() {
        println("$this: I am not afraid of dying; any time will do; I don't mind")
        Slaktor.kill(extractorAddr!!)
    }

}

class Loader : AbstractActor() {

    val transformerAddr = Slaktor.spawn(Transformer::class.java)

    private fun askForData() {
        if (transformerAddr == null) return
        Slaktor.send(DataRequest(500, this.address), transformerAddr)
    }

    private var recordsLoaded = 0

    override fun processMessage(message: Any) {
        when (message) {
            is DataRecord -> {
                recordsLoaded++
                println("Loaded $message, number $recordsLoaded")
                if (recordsLoaded >= 500) {
                    println("asking for more data")
                    Slaktor.kill(transformerAddr!!)
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
    override fun prepareToDie() {
        println("It's my life; it never ends")
    }

}

fun main(args: Array<String>) {

    Slaktor.register(Extractor::class.java, {
        println("Creating extractor")
        Extractor()
    })

    Slaktor.register(Transformer::class.java, {
        println("Creating transformer")
        Transformer()
    })

    Slaktor.register(Loader::class.java, {
        println("Creating loader")
        Loader()
    })

    val loaderAddr = Slaktor.spawn(Loader::class.java)
    val l2 = Slaktor.spawn(Loader::class.java)
    val l3 = Slaktor.spawn(Loader::class.java)
    Thread.sleep(5000)
    Slaktor.broadcastToInstancesOf(Loader::class.java, StartMsg)
    Thread.sleep(10000)
    Slaktor.killAllInstancesOf(Loader::class.java)

}
