package com.github.chrix75.unifiedlog.storage.h2

import org.h2.jdbcx.JdbcDataSource
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test

class H2OffsetStorageTest {

    private lateinit var offsetStorage: H2OffsetStorage
    private lateinit var ds: JdbcDataSource
    @Before
    fun setUp() {
        ds = JdbcDataSource()
        ds.setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
        dropOffsetTable(ds)
        offsetStorage = H2OffsetStorage(ds, "TEST_WALKER_OFFSET")
    }

    @Test
    fun get_offset_without_previous_value() {
        assertEquals(0, offsetStorage.value)
    }

    @Test
    fun fetch_previously_saved_offset() {
        ++offsetStorage
        ++offsetStorage
        ++offsetStorage
        assertEquals(3, offsetStorage.value)

        val other = H2OffsetStorage(ds, "TEST_WALKER_OFFSET")
        assertEquals(3, other.value)
    }

    private fun dropOffsetTable(ds: JdbcDataSource) {
        ds.connection.use { c ->
            c.prepareStatement("DROP TABLE IF EXISTS WALKER_OFFSET").use { it.execute() }
        }
    }
}