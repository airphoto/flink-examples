/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

package example.java.sql.formats.binlog;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.descriptors.DescriptorProperties;
import com.lhs.flink.example.java.sql.formats.descriptors.LogValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Table format factory for providing configured instances of JSON-to-row {@link SerializationSchema}
 * and {@link DeserializationSchema}.
 */
public class LogRowFormatFactory extends TableFormatFactoryBase<Row>
		implements SerializationSchemaFactory<Row>, DeserializationSchemaFactory<Row> {

	public LogRowFormatFactory() {
		super(LogValidator.FORMAT_TYPE_VALUE, 1, true);
	}

	@Override
	protected List<String> supportedFormatProperties() {
		final List<String> properties = new ArrayList<>();
		properties.add(LogValidator.FORMAT_JSON_SCHEMA);
		properties.add(LogValidator.FORMAT_SCHEMA);
		properties.add(LogValidator.FORMAT_FAIL_ON_MISSING_FIELD);
		return properties;
	}

	@Override
	public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		// create and configure
		final LogRowDeserializationSchema.Builder schema =
			new LogRowDeserializationSchema.Builder(createTypeInformation(descriptorProperties));

		descriptorProperties.getOptionalBoolean(LogValidator.FORMAT_FAIL_ON_MISSING_FIELD)
			.ifPresent(flag -> {
				if (flag) {
					schema.failOnMissingField();
				}
			});

		return schema.build();
	}

	@Override
	public SerializationSchema<Row> createSerializationSchema(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		// create and configure
		return new LogRowSerializationSchema.Builder(createTypeInformation(descriptorProperties)).build();
	}

	private TypeInformation<Row> createTypeInformation(DescriptorProperties descriptorProperties) {
		if (descriptorProperties.containsKey(LogValidator.FORMAT_SCHEMA)) {
			return (RowTypeInfo) descriptorProperties.getType(LogValidator.FORMAT_SCHEMA);
		} else if (descriptorProperties.containsKey(LogValidator.FORMAT_JSON_SCHEMA)) {
			return LogRowSchemaConverter.convert(descriptorProperties.getString(LogValidator.FORMAT_JSON_SCHEMA));
		} else {
			return deriveSchema(descriptorProperties.asMap()).toRowType();
		}
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> propertiesMap) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putProperties(propertiesMap);

		// validate
		new LogValidator().validate(descriptorProperties);

		return descriptorProperties;
	}
}
