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

import org.apache.nifi.annotation.behavior.InputRequirement
import org.apache.nifi.annotation.behavior.ReadsAttribute
import org.apache.nifi.annotation.behavior.SideEffectFree
import org.apache.nifi.annotation.behavior.WritesAttribute
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.documentation.Tags
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.*
import org.apache.nifi.processor.util.StandardValidators
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripperByArea
import java.awt.geom.Rectangle2D
import java.io.OutputStream
import java.security.InvalidParameterException
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashSet


val RELATION_SUCCESS: Relationship = Relationship.Builder().apply {
    name("Success")
    description("Outputs the content")
}.build()

val RELATION_FAILURE: Relationship = Relationship.Builder().apply {
    name("Failure")
    description("Outputs the content on Failure")
}.build()

val PROPERTY_OPERATION: PropertyDescriptor = PropertyDescriptor.Builder().apply {
    name("Operation")
    description("Operation for text extraction")
    defaultValue(StripperTypes.Text2HTML.name)
    required(true)
    allowableValues(StripperTypes.values().map { it.name }.toSet())
}.build()

val PROPERTY_START_PAGE: PropertyDescriptor = PropertyDescriptor.Builder().apply {
    name("Start Page")
    description("Page where to start Text Extraction")
    defaultValue("1")
    required(true)
    addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
}.build()

val PROPERTY_END_PAGE: PropertyDescriptor = PropertyDescriptor.Builder().apply {
    name("End Page subtractor")
    description("Value for subtracting the last page from to determine the last Page")
    defaultValue("0")
    required(true)
    addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
}.build()

@Tags("pdf", "exctract", "text")
@CapabilityDescription("Extract pdf text from incoming flowfile pdf")
@ReadsAttribute(attribute = "mime.type", description = "must be application/pdf")
@WritesAttribute(attribute = "mime.type", description = "writes plain/text as new content type")
@SideEffectFree
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
class ExtractPDF : AbstractProcessor() {

    private lateinit var properties: MutableList<PropertyDescriptor>

    override fun getSupportedPropertyDescriptors(): MutableList<PropertyDescriptor> {
        return properties
    }

    private lateinit var relations: MutableSet<Relationship>

    override fun getRelationships(): MutableSet<Relationship> {
        return relations
    }

    override fun getSupportedDynamicPropertyDescriptor(propertyDescriptorName: String?): PropertyDescriptor {
        return PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(
                        StandardValidators.createListValidator(
                                true,
                                true,
                                StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
                )
                .dynamic(true)
                .expressionLanguageSupported(true)
                .build()
    }

    override fun init(context: ProcessorInitializationContext?) {
        properties = ArrayList()
        properties.add(PROPERTY_OPERATION)
        properties.add(PROPERTY_START_PAGE)
        properties.add(PROPERTY_END_PAGE)
        properties = Collections.unmodifiableList(properties)

        relations = HashSet()
        relations.add(RELATION_SUCCESS)
        relations.add(RELATION_FAILURE)
        relations = Collections.unmodifiableSet(relations)
    }

    override fun onTrigger(context: ProcessContext, session: ProcessSession?) {
        val inFile = session!!.get()
        try {
            if(inFile.getAttribute("mime.type") != "application/pdf")
                throw InvalidParameterException("Mime type is not set to application/pdf")

            val flowFileReader = session.read(inFile)
            flowFileReader.use {
                val doc = PDDocument.load(flowFileReader)
                val stripperType = StripperTypes.valueOf(context.getProperty(PROPERTY_OPERATION).value)
                val stripper = stripperType.createInstance()
                stripper.startPage = context.getProperty(PROPERTY_START_PAGE).asInteger()
                stripper.endPage = doc.numberOfPages - context.getProperty(PROPERTY_END_PAGE).asInteger()
                var textOut = ""
                if (stripperType == StripperTypes.TextStripperByArea) {
                    addStipperTextAreas(stripper as PDFTextStripperByArea, context)
                    for (i in stripper.startPage .. stripper.endPage) {
                        val pdPage = doc.getPage(i - 1)
                        stripper.extractRegions(pdPage)
                        for (region in stripper.regions) {
                            generateFlowFile(session, inFile, stripper.getTextForRegion(region), region, "plain/text")
                        }
                    }
                } else {
                    textOut = stripper.getText(doc)
                }

                if(stripperType == StripperTypes.Text2HTML)
                    generateFlowFile(session, inFile, textOut, mimeType = "text/html")
                else if (stripperType == StripperTypes.TextStripper)
                    generateFlowFile(session, inFile, textOut)
            }
            session.remove(inFile)
        } catch (e: Exception) {
            session.transfer(inFile, RELATION_FAILURE)
            logger.error(e.message, e)
        }
    }

    private fun generateFlowFile(session: ProcessSession, inFile: FlowFile?, textOut: String, region: String = "", mimeType: String = "plain/text") {
        var outFile = session.create(inFile)
        outFile = session.putAttribute(outFile,"mime.type", mimeType)
        if (region != "")
            outFile = session.putAttribute(outFile, "pdf.region", region)

        session.write(outFile).use { outputStream: OutputStream ->
            outputStream.write(textOut.toByteArray(Charsets.UTF_8))
        }
        session.transfer(outFile, RELATION_SUCCESS)
    }

    private fun addStipperTextAreas(stripper: PDFTextStripperByArea, context: ProcessContext) {
        context.properties.entries.filter { it.key.isDynamic }.forEach {
            val coordParts = it.value.split(',')
            if (coordParts.size != 4) {
                throw InvalidParameterException("Property format for ${it.key.name} is invalid x,y,height,with expected")
            }

            val x = coordParts[0].toDouble()
            val y = coordParts[1].toDouble()

            val w = coordParts[2].toDouble()
            val h = coordParts[3].toDouble()

            stripper.addRegion(it.key.name, Rectangle2D.Double(x, y, w, h))
        }
    }
}