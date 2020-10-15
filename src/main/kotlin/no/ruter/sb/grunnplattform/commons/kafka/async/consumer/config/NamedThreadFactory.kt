package no.ruter.sb.grunnplattform.commons.kafka.async.consumer.config

import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

class NamedThreadFactory(private val baseName: String, private val appendThreadNum: Boolean = true) : ThreadFactory {

    private var threadNum = AtomicInteger(0)

    override fun newThread(runnable: Runnable): Thread {
        return Thread(runnable, getName())
    }

    private fun getName() = if (appendThreadNum) "$baseName-${threadNum.incrementAndGet()}" else baseName

}