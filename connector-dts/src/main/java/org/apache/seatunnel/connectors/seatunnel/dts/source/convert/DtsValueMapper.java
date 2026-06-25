package org.apache.seatunnel.connectors.seatunnel.dts.source.convert;

import com.aliyun.dts.subscribe.clients.record.RecordSchema;
import com.aliyun.dts.subscribe.clients.record.RowImage;
import com.aliyun.dts.subscribe.clients.record.value.Value;

public final class DtsValueMapper {

    private DtsValueMapper() {}

    public static Object toJava(Value value) {
        if (value == null) {
            return null;
        }
        String text = value.toString();
        if (text == null || "null".equalsIgnoreCase(text)) {
            return null;
        }
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException ignored) {
            // continue
        }
        try {
            return Double.parseDouble(text);
        } catch (NumberFormatException ignored) {
            // continue
        }
        if ("true".equalsIgnoreCase(text) || "false".equalsIgnoreCase(text)) {
            return Boolean.parseBoolean(text);
        }
        return text;
    }

    public static String toJsonValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        return "\"" + escapeJson(value.toString()) + "\"";
    }

    public static String escapeJson(String value) {
        return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    public static String rowImageToJson(RecordSchema schema, RowImage image) {
        if (image == null || schema == null) {
            return "{}";
        }
        StringBuilder json = new StringBuilder("{");
        boolean first = true;
        for (int i = 0; i < schema.getFieldCount(); i++) {
            String fieldName = schema.getField(i).getFieldName();
            Object javaValue = toJava(image.getValue(i));
            if (!first) {
                json.append(',');
            }
            json.append('"').append(escapeJson(fieldName)).append("\":");
            json.append(toJsonValue(javaValue));
            first = false;
        }
        json.append('}');
        return json.toString();
    }
}
