package com.github.chrix75.unifiedlog.storage.h2

import csperandio.unifiedlog.events.EventBuilder
import csperandio.unifiedlog.events.SimpleEventType
import org.h2.jdbcx.JdbcDataSource
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test

class H2EventStorageTest {

    private lateinit var storage: H2EventStorage

    @Before
    fun setUp() {
        val ds = JdbcDataSource()
        ds.setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
        dropEventTable(ds)
        storage = H2EventStorage(ds, StringEventTypeBuilder(), 5)
        storage.createTablesIfNotExist()
    }

    private fun dropEventTable(ds: JdbcDataSource) {
        ds.connection.use {c ->
            c.prepareStatement("DROP TABLE IF EXISTS LOG_EVENT").use { it.execute() }
        }
    }

    @Test
    fun save_one_event() {
        storage.save(
            EventBuilder().fromString(SimpleEventType("SAVE_TESTED"), "A NEW EVENT")
        )

        val event = storage[0]
        assertEquals("A NEW EVENT", String(event.data))
    }

    @Test(expected = IndexOutOfBoundsException::class)
    fun get_unknown_event() {
        storage[0]
    }

    @Test
    fun events_in_one_page() {
        createEvents(4)
        val eventValues = readEvents(4)
        assertEquals("EVENT 1;EVENT 2;EVENT 3;EVENT 4;", eventValues)
    }

    @Test
    fun events_in_complete_page() {
        createEvents(5)
        val eventValues = readEvents(5)
        assertEquals("EVENT 1;EVENT 2;EVENT 3;EVENT 4;EVENT 5;", eventValues)

    }

    @Test
    fun events_dispatched_on_two_pages() {
        createEvents(7)
        val eventValues = readEvents(7)
        assertEquals("EVENT 1;EVENT 2;EVENT 3;EVENT 4;EVENT 5;EVENT 6;EVENT 7;", eventValues)
    }

    @Test
    fun search_individual_event() {
        createEvents(7)
        val e1 = storage[0]
        assertEquals("EVENT 1", String(e1.data))
        val e4 = storage[3]
        assertEquals("EVENT 4", String(e4.data))
        val e7 = storage[6]
        assertEquals("EVENT 7", String(e7.data))
    }

    @Test
    fun search_unknown_event_and_after_add_it() {
        createEvents(7)
        try {
            storage[7]
            fail("Event 8 must be not found")
        } catch (e: IndexOutOfBoundsException) {}

        storage.save(
            EventBuilder().fromString(SimpleEventType("SAVE_TESTED"), "EVENT 8")
        )

        val e = storage[7]
        assertEquals("EVENT 8", String(e.data))
    }


    private fun readEvents(count: Int): String {
        val buf = StringBuffer()
        for (i in 0 until count) {
            val e = storage[i]
            buf.append(String(e.data) + ";")
        }
        return buf.toString()
    }

    private fun createEvents(count: Int) {
        for (i in 0 until count) {
            storage.save(
                EventBuilder().fromString(SimpleEventType("SAVE_TESTED"), "EVENT ${i + 1}")
            )
        }
    }
}

