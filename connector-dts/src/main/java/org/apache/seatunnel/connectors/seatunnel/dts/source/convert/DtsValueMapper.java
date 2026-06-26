package org.apache.seatunnel.connectors.seatunnel.dts.source.convert;

import com.aliyun.dts.subscribe.clients.record.RecordSchema;
import com.aliyun.dts.subscribe.clients.record.RowImage;
import com.aliyun.dts.subscribe.clients.record.value.Value;

public final class DtsValueMapper {

    private DtsValueMapper() {}

    /** 尽力将 DTS {@link Value} 转为 Java 标量，供下游使用。 */
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

    /** 将 Java 值序列化为 JSON 字面量（字符串加引号，数字/布尔不加引号）。 */
    public static String toJsonValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        return "\"" + escapeJson(value.toString()) + "\"";
    }

    /**
     * 转义 JSON 字符串中的控制字符。使用单次 {@link StringBuilder} 扫描，避免 {@link
     * String#replace} 在大字段（TEXT/BLOB）上反复编译正则并产生巨大中间字符串导致 OOM。
     */
    public static String escapeJson(String value) {
        if (value == null || value.isEmpty()) {
            return value == null ? "" : value;
        }
        int len = value.length();
        StringBuilder sb = new StringBuilder(len + 16);
        for (int i = 0; i < len; i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\':
                    sb.append("\\\\");
                    break;
                case '"':
                    sb.append("\\\"");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    sb.append(c);
            }
        }
        return sb.toString();
    }

    /** 构建 {@code _columns_json}，不限制单列长度（{@code maxTextLength=0}）。 */
    public static String rowImageToJson(RecordSchema schema, RowImage image) {
        return rowImageToJson(schema, image, 0);
    }

    /**
     * 根据 schema 列名与行镜像值构建 {@code _columns_json}（{@code {"col":value,...}}）。{@code
     * maxTextLength} 会在转义前截断超长字符串列。
     */
    public static String rowImageToJson(RecordSchema schema, RowImage image, int maxTextLength) {
        if (image == null || schema == null) {
            return "{}";
        }
        Value[] values = image.getValues();
        int fieldCount = schema.getFieldCount();
        if (values == null || values.length == 0 || fieldCount == 0) {
            return "{}";
        }
        int count = Math.min(fieldCount, values.length);
        StringBuilder json = new StringBuilder(count * 24 + 2);
        json.append('{');
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                json.append(',');
            }
            json.append('"').append(escapeJson(schema.getField(i).getFieldName())).append("\":");
            json.append(valueToJson(values[i], maxTextLength));
        }
        json.append('}');
        return json.toString();
    }

    static String valueToJson(Value value) {
        return valueToJson(value, 0);
    }

    /** 序列化单个 DTS 列值；数字/布尔以不加引号的 JSON 字面量输出。 */
    static String valueToJson(Value value, int maxTextLength) {
        if (value == null) {
            return "null";
        }
        String text = value.toString();
        if (text == null || "null".equalsIgnoreCase(text)) {
            return "null";
        }
        try {
            Long.parseLong(text);
            return text;
        } catch (NumberFormatException ignored) {
            // continue
        }
        try {
            Double.parseDouble(text);
            return text;
        } catch (NumberFormatException ignored) {
            // continue
        }
        if ("true".equalsIgnoreCase(text) || "false".equalsIgnoreCase(text)) {
            return text;
        }
        text = truncateText(text, maxTextLength);
        return "\"" + escapeJson(text) + "\"";
    }

    /** {@code maxTextLength > 0} 时截断超长文本；{@code 0} 表示不限制。 */
    static String truncateText(String text, int maxTextLength) {
        if (maxTextLength <= 0 || text.length() <= maxTextLength) {
            return text;
        }
        return text.substring(0, maxTextLength) + "...[truncated]";
    }
}
