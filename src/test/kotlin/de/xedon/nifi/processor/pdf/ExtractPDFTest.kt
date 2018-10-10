/*
 * Copyright 2018 Xedon
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

package de.xedon.nifi.processor.pdf

import org.apache.nifi.util.MockFlowFile
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.pdmodel.PDPage
import org.apache.pdfbox.pdmodel.PDPageContentStream
import org.apache.pdfbox.pdmodel.font.PDType1Font
import org.hamcrest.core.IsInstanceOf.instanceOf
import org.hamcrest.text.IsEqualIgnoringWhiteSpace.equalToIgnoringWhiteSpace
import org.junit.Assert.*
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import java.io.ByteArrayOutputStream
import java.nio.file.Paths
import java.security.InvalidParameterException

class ExtractPDFTest {
    companion object {
        val pdfStream :ByteArrayOutputStream = ByteArrayOutputStream()
        @BeforeClass
        @JvmStatic
        fun preparePDF() {
            val doc = PDDocument()
            addTestPage(doc)
            addTestPage(doc)
            doc.save(pdfStream)
            doc.save(Paths.get("build","tmp","test.pdf").toFile())
        }

        private fun addTestPage(doc: PDDocument) {
            val page = PDPage()
            val topY = page.mediaBox.upperRightY
            val topX = page.mediaBox.lowerLeftX
            PDPageContentStream(doc, page).use {
                it.beginText()
                it.setFont(PDType1Font.TIMES_ROMAN, 12f)
                it.setLeading(12f)
                it.newLineAtOffset(topX, topY)
                it.newLine()
                it.showText("Row 1")
                it.newLine()
                it.showText("Row 2")
                it.newLine()
                it.showText("Row 3")
                it.newLine()
                it.showText("Row 4")
                it.endText()
            }
            doc.addPage(page)
        }
    }

    private lateinit var runner: TestRunner

    @Before
    fun prepareTest()
    {
        runner = TestRunners.newTestRunner(ExtractPDF::class.java)
        runner.setIncomingConnection(true)
        runner.setRelationshipAvailable(RELATION_FAILURE)
        runner.setRelationshipAvailable(RELATION_SUCCESS)
    }

    @Test
    fun testMimeTypeError()
    {
        runner.enqueue(pdfStream.toByteArray(), mutableMapOf("mime.type" to "application/json"))
        runner.setProperty(PROPERTY_OPERATION, StripperTypes.TextStripper.name)
        runner.setProperty(PROPERTY_START_PAGE,"1")
        runner.setProperty(PROPERTY_END_PAGE,"0")
        runner.run()
        runner.assertQueueEmpty()
        runner.assertTransferCount(RELATION_FAILURE,1)
        //Expect 2 messages caused by a test framework bug
        assertEquals(2,runner.logger.errorMessages.size)
        assertThat(runner.logger.errorMessages[1].throwable, instanceOf(InvalidParameterException::class.java))
    }

    @Test
    fun testSimplePDFExtraction() {
        runner.enqueue(pdfStream.toByteArray(), mutableMapOf("mime.type" to "application/pdf"))
        runner.setProperty(PROPERTY_OPERATION, StripperTypes.TextStripper.name)
        runner.setProperty(PROPERTY_START_PAGE,"1")
        runner.setProperty(PROPERTY_END_PAGE,"1")
        runner.run()
        runner.assertQueueEmpty()
        runner.assertTransferCount(RELATION_FAILURE,0)
        runner.assertTransferCount(RELATION_SUCCESS,1)
        runner.assertAllFlowFiles (RELATION_SUCCESS) {
            assertTrue(it is MockFlowFile)
            assertEquals("plain/text",it.attributes["mime.type"])
            (it as MockFlowFile).assertContentEquals(
                    "Row 1${System.lineSeparator()}" +
                            "Row 2${System.lineSeparator()}" +
                            "Row 3${System.lineSeparator()}" +
                            "Row 4${System.lineSeparator()}",
                     Charsets.UTF_8
            )
        }

    }

    @Test
    fun testHtmlPDFExtraction() {
        runner.enqueue(pdfStream.toByteArray(), mutableMapOf("mime.type" to "application/pdf"))
        runner.setProperty(PROPERTY_OPERATION, StripperTypes.Text2HTML.name)
        runner.setProperty(PROPERTY_START_PAGE,"1")
        runner.setProperty(PROPERTY_END_PAGE,"1")
        runner.run()
        runner.assertQueueEmpty()
        runner.assertTransferCount(RELATION_FAILURE,0)
        runner.assertTransferCount(RELATION_SUCCESS,1)
        runner.assertAllFlowFiles (RELATION_SUCCESS) {
            assertTrue(it is MockFlowFile)
            assertEquals("text/html",it.attributes["mime.type"])
            assertThat(
                    Paths.get("src","test","resources","test.html").toFile().readText(),
                     equalToIgnoringWhiteSpace((it as MockFlowFile).toByteArray().toString(Charsets.UTF_8))
            )
        }

    }

    @Test
    fun testSimpleTextAreaPDFExtraction() {
        runner.enqueue(pdfStream.toByteArray(), mutableMapOf("mime.type" to "application/pdf"))
        runner.setProperty(PROPERTY_OPERATION, StripperTypes.TextStripperByArea.name)
        runner.setProperty(PROPERTY_START_PAGE,"1")
        runner.setProperty(PROPERTY_END_PAGE,"0")
        //                              x y w h
        runner.setProperty("TEST_AREA","0,0,200,20")
        runner.run()
        runner.assertQueueEmpty()
        runner.assertTransferCount(RELATION_FAILURE,0)
        runner.assertTransferCount(RELATION_SUCCESS,2)
        runner.assertAllFlowFiles (RELATION_SUCCESS) {
            assertTrue(it is MockFlowFile)
            assertEquals("plain/text",it.attributes["mime.type"])
            assertEquals("TEST_AREA",it.attributes["pdf.region"])
            (it as MockFlowFile).assertContentEquals(
                    "Row 1${System.lineSeparator()}",
                    Charsets.UTF_8
            )
        }

    }
}