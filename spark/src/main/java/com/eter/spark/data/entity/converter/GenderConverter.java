package com.eter.spark.data.entity.converter;

import com.eter.spark.data.entity.Gender;

import javax.persistence.AttributeConverter;

/**
 * Converter for {@link Gender} enum.
 * Used for convert enum name to non case sensitive.
 * For example: "MALE" to "male", or it's the same value.
 *
 * @see Gender
 * @see AttributeConverter
 */
public class GenderConverter implements AttributeConverter<Gender, String> {

    /**
     * Convert enum value of {@link Gender} to database string value.
     * For example: "MALE" to "male".
     *
     * @param gender value to convert
     * @return string value of {@link Gender} enum
     */
    public String convertToDatabaseColumn(Gender gender) {
        return gender.valueName();
    }

    /**
     * Convert from string value to {@link Gender} enum value.
     * For example: from "male" to {@link Gender#MALE}.
     *
     * @param s string value
     * @return {@link Gender} value
     */
    public Gender convertToEntityAttribute(String s) {
        return Gender.getByValue(s);
    }
}
