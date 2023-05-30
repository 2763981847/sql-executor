package cn.autumnclouds.sql.util;

import com.alibaba.druid.util.StringUtils;

import java.math.BigDecimal;

/**
 * @author Oreki
 * @since 2023/5/29
 */
public class ConvertUtils {
    public static Object convertValue(String dataType, String strValue) {
        if (strValue == null || dataType == null) {
            return null;
        }
        dataType = dataType.toLowerCase();
        if (StringUtils.isEmpty(strValue) && (!"string".equals(dataType) && !"varchar".equals(dataType))) {
            return null;
        }

        // use data type enum instead of string comparison
        if ("date".equals(dataType)) {
            return DateUtils.stringToDate(strValue);
        } else if ("tinyint".equals(dataType)) {
            return Byte.valueOf(strValue);
        } else if ("short".equals(dataType) || "smallint".equals(dataType)) {
            return Short.valueOf(strValue);
        } else if ("integer".equals(dataType)) {
            return Integer.valueOf(strValue);
        } else if ("long".equals(dataType) || "bigint".equals(dataType)) {
            return Long.valueOf(strValue);
        } else if ("double".equals(dataType)) {
            return Double.valueOf(strValue);
        } else if ("decimal".equals(dataType)) {
            return new BigDecimal(strValue);
        } else if ("timestamp".equals(dataType)) {
            return DateUtils.stringToMillis(strValue);
        } else if ("float".equals(dataType)) {
            return Float.valueOf(strValue);
        } else if ("boolean".equals(dataType)) {
            return Boolean.valueOf(strValue);
        } else {
            return strValue;
        }
    }
}
