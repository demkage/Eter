package com.eter.spark.data.entity;

import com.eter.spark.data.entity.converter.GenderConverter;

import javax.persistence.*;

/**
 * Describe customer additional details from database.
 * Is referenced by {@link Customer} class.
 *
 * @see Customer
 */
@Entity
@Table(name = "customersdetail")
public class CustomerDetail {
    private Long id;
    private Gender gender;
    private Integer age;

    /**
     * Return unique identifier of entity.
     *
     * @return unique identifier
     */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    public Long getId() {
        return id;
    }

    /**
     * Set unique identifier for class.
     * Is not recommended for use.
     *
     * @param id unique identifier
     */
    public void setId(Long id) {
        this.id = id;
    }

    /**
     * Return customer gender in string format.
     * For example: "male", "female", etc.
     * Use @see com.eter.spark.data.entity.converter.GenderConverter
     * to not be case sensitive.
     *
     * @return customer gender
     */
    @Convert(converter = GenderConverter.class)
    @Column(name = "gender")
    @Enumerated(EnumType.STRING)
    public Gender getGender() {
        return gender;
    }

    /**
     * Set customer gender in string format.
     * For example: "male", "female", etc.
     *
     * @param gender customer gender
     */
    public void setGender(Gender gender) {
        this.gender = gender;
    }

    /**
     * Return customer age.
     *
     * @return customer age
     */
    @Column(name = "age")
    public Integer getAge() {
        return age;
    }

    /**
     * Set customer age.
     *
     * @param age customer age
     */
    public void setAge(Integer age) {
        this.age = age;
    }
}
