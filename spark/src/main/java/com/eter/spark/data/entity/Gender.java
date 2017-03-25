package com.eter.spark.data.entity;

/**
 * Enum value for gender types.
 */
public enum Gender {
    MALE("male"),
    FEMALE("female"),
    UNKNOWN("unknown");

    /**
     * Value of gender in lowercase.
     */
    private String value;

    Gender(String value) {
        this.value = value;
    }

    /**
     * Return {@link Gender} enum value by his string equivalent.
     *
     * @param value string equivalent
     * @return {@link Gender} enum value
     */
    public static Gender getByValue(String value) {
        for (Gender gender : Gender.values()) {
            if (gender.valueName().toLowerCase().equals(value.toLowerCase()))
                return gender;
        }

        return Gender.UNKNOWN;
    }

    /**
     * Return string value of enum.
     *
     * @return string value of enum
     */
    public String valueName() {
        return value;
    }
}
