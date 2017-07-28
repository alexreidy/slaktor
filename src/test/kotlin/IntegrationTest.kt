import slaktor.AbstractActor
import slaktor.Actor

data class DataRequest(val recordCount: Int, val sender: Actor)

data class DataRecord(val data: String)

object StartMsg

class Extractor : AbstractActor() {

    override fun processMessage(message: Any) {
        if (message is DataRequest) {
            for (i in 1..message.recordCount) {
                message.sender.inbox.addMessage(DataRecord("$i"))
                Thread.sleep(5)
            }
        }
    }

    override fun performIdleTask() {
        println("$this is idle")
    }

    override fun prepareToDie() {}

}

class Transformer : AbstractActor() {

    var extractor: Actor? = null

    var buffer = ArrayList<DataRecord>()

    var destination: Actor? = null

    fun flushBuffer() {
        destination?.inbox?.addMessages(buffer)
        buffer = ArrayList()
    }

    override fun processMessage(message: Any) {
        when (message) {
            is DataRecord -> {
                buffer.add(message)
                if (buffer.size > 500) {

                }
            }
            is DataRequest -> {
                destination = message.sender
                extractor?.inbox?.addMessage(DataRequest(message.recordCount, this))
            }
        }
    }

    private var timesBufferNotEmpty = 0

    override fun performIdleTask() {
        println("$this is idle")

        if (buffer.isNotEmpty()) {
            timesBufferNotEmpty++
            if (timesBufferNotEmpty > 2) {
                println("!!!!!!! flushing buffer !!!!!!!")
                flushBuffer()
                timesBufferNotEmpty = 0
            }
        }
    }

    override fun prepareToDie() {}

}

class Loader : AbstractActor() {

    var transformer: Actor? = null

    private fun askForData() {
        transformer?.inbox?.addMessage(DataRequest(5000, this))
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
                askForData()
            }
        }
    }

    override fun performIdleTask() {
        println("$this is idle")
    }

    override fun prepareToDie() {}

}

fun main(args: Array<String>) {

    val extractor = Extractor()

    val transformer = Transformer()
    transformer.extractor = extractor

    val loader = Loader()
    loader.transformer = transformer

    loader.inbox.addMessage(StartMsg)

}
