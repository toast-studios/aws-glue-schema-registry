/*
 * Copyright 2014 Confluent Inc.
 * Portions Copyright 2020 Amazon.com, Inc. or its affiliates.
 * All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xyz.converters.avrodata;

import org.apache.avro.Schemas;
import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroSchema implements ParsedSchema {

    private static final Logger log = LoggerFactory.getLogger(AvroSchema.class);

    public static final String TYPE = "AVRO";

    private static final SchemaValidator BACKWARD_VALIDATOR = new SchemaValidatorBuilder().canReadStrategy()
            .validateLatest();

    private final Schema schemaObj;
    private String canonicalString;
    private final Integer version;
    private final Map<String, String> resolvedReferences;
    private final boolean isNew;

    public AvroSchema(String schemaString) {
        this(schemaString, Collections.emptyMap(), null);
    }

    public AvroSchema(String schemaString,
            Map<String, String> resolvedReferences,
            Integer version) {
        this(schemaString,
                resolvedReferences, version, false);
    }

    public AvroSchema(String schemaString,
            Map<String, String> resolvedReferences,
            Integer version,
            boolean isNew) {
        this.isNew = isNew;
        Schema.Parser parser = getParser();
        for (String schema : resolvedReferences.values()) {
            parser.parse(schema);
        }
        this.schemaObj = parser.parse(schemaString);
        this.resolvedReferences = Collections.unmodifiableMap(resolvedReferences);
        this.version = version;
    }

    public AvroSchema(Schema schemaObj) {
        this(schemaObj, null);
    }

    public AvroSchema(Schema schemaObj, Integer version) {
        this.isNew = false;
        this.schemaObj = schemaObj;
        this.resolvedReferences = Collections.emptyMap();
        this.version = version;
    }

    private AvroSchema(
            Schema schemaObj,
            String canonicalString,
            Map<String, String> resolvedReferences,
            Integer version,
            boolean isNew) {
        this.isNew = isNew;
        this.schemaObj = schemaObj;
        this.canonicalString = canonicalString;
        this.resolvedReferences = resolvedReferences;
        this.version = version;
    }

    public AvroSchema copy() {
        return new AvroSchema(
                this.schemaObj,
                this.canonicalString,
                this.resolvedReferences,
                this.version,
                this.isNew);
    }

    protected Schema.Parser getParser() {
        Schema.Parser parser = new Schema.Parser();
        parser.setValidateDefaults(isNew());
        return parser;
    }

    @Override
    public Schema rawSchema() {
        return schemaObj;
    }

    @Override
    public String schemaType() {
        return TYPE;
    }

    @Override
    public String name() {
        if (schemaObj != null && schemaObj.getType() == Schema.Type.RECORD) {
            return schemaObj.getFullName();
        }
        return null;
    }

    @Override
    public String canonicalString() {
        if (schemaObj == null) {
            return null;
        }
        if (canonicalString == null) {
            Schema.Parser parser = getParser();
            List<Schema> schemaRefs = new ArrayList<>();
            for (String schema : resolvedReferences.values()) {
                Schema schemaRef = parser.parse(schema);
                schemaRefs.add(schemaRef);
            }
            canonicalString = Schemas.toString(schemaObj, schemaRefs);
        }
        return canonicalString;
    }

    public Integer version() {
        return version;
    }

    public Map<String, String> resolvedReferences() {
        return resolvedReferences;
    }

    public boolean isNew() {
        return isNew;
    }

    @Override
    public List<String> isBackwardCompatible(ParsedSchema previousSchema) {
        if (!schemaType().equals(previousSchema.schemaType())) {
            return Collections.singletonList("Incompatible because of different schema type");
        }
        try {
            BACKWARD_VALIDATOR.validate(this.schemaObj,
                    Collections.singleton(((AvroSchema) previousSchema).schemaObj));
            return Collections.emptyList();
        } catch (SchemaValidationException e) {
            return Collections.singletonList(e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected exception during compatibility check", e);
            return Collections.singletonList(
                    "Unexpected exception during compatibility check: " + e.getMessage());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AvroSchema that = (AvroSchema) o;
        return Objects.equals(schemaObj, that.schemaObj) && Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaObj, version);
    }

    @Override
    public String toString() {
        return canonicalString();
    }
}
