import org.junit.Assert
import org.junit.Test
import slaktor.StandardInbox

class UnitTests {

    @Test
    fun testInbox() {
        val inbox = StandardInbox<Int>()

        var messageAddedCount = 0
        inbox.messagesAddedEvent.addHandler {
            messageAddedCount++
        }

        val ACTUAL_MESSAGE_ADDED_COUNT = 100
        for (i in 1..ACTUAL_MESSAGE_ADDED_COUNT) {
            inbox.addMessage(i)
        }

        Assert.assertTrue(messageAddedCount == ACTUAL_MESSAGE_ADDED_COUNT)

        var message: Int? = inbox.nextMessage
        var messageCount = 0
        var previousNumber = 0
        while (message != null) {
            messageCount++
            // Since the messages were added to the inbox by one thread,
            // we should expect them to be ordered FIFO
            Assert.assertTrue(message > previousNumber)
            previousNumber = message
            message = inbox.nextMessage
        }

        Assert.assertTrue(messageCount == ACTUAL_MESSAGE_ADDED_COUNT)
    }



}