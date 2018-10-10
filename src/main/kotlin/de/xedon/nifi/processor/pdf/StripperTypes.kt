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

import org.apache.pdfbox.text.PDFTextStripper
import org.apache.pdfbox.text.PDFTextStripperByArea
import org.apache.pdfbox.tools.PDFText2HTML
import kotlin.reflect.KClass

enum class StripperTypes(private val pdfStreamEngine: KClass<*>) {
    TextStripper(PDFTextStripper::class),

    TextStripperByArea(PDFTextStripperByArea::class),

    Text2HTML(PDFText2HTML::class);

    fun createInstance() : PDFTextStripper
    {
        return this.pdfStreamEngine.java.newInstance() as PDFTextStripper
    }
}