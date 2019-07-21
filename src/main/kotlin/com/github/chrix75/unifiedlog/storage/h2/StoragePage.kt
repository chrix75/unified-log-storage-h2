package com.github.chrix75.unifiedlog.storage.h2

import csperandio.unifiedlog.events.Event
import java.util.*
import javax.sql.DataSource

class StoragePage(private val pageSize: Int, private val ds: DataSource, private val typeBuilder: EventTypeBuilder) {

    private var window = IntRange(0, pageSize - 1)
    private val currentPage = mutableListOf<Event>()

    operator fun get(i: Int): Event {
        if (mustReadPage(i)) {
            readPage(i)
        }

        val offset = i - window.start
        if (offset >= currentPage.size) {
            throw IndexOutOfBoundsException("No Event at index $i")
        }

        return currentPage[offset]
    }

    private fun mustReadPage(i: Int): Boolean {
        if (currentPage.isEmpty()) {
            return true
        }

        return !window.contains(i)
    }

    private fun readPage(i: Int) {
        currentPage.clear()
        window = IntRange(i, i + pageSize - 1)

        ds.connection.use { c ->
            val st = c.prepareStatement(
                "SELECT * FROM LOG_EVENT " +
                        "ORDER BY EVENT_SEQ LIMIT $pageSize OFFSET ${window.first}"
            )

            st.use {
                it.executeQuery().use { rs ->
                    while(rs.next()) {
                        val type = typeBuilder.from(rs.getString("EVENT_TYPE"))
                        val id = UUID.fromString(rs.getString("EVENT_ID"))
                        val data = rs.getString("EVENT_DATA").toByteArray()
                        val timestamp = rs.getDate("EVENT_TIMESTAMP")

                        val e = Event(type, data, id, timestamp)
                        currentPage.add(e)
                    }
                }
            }
        }
    }
}
