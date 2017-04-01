package com.eter.spark.data.entity;

import javax.persistence.*;
import java.io.Serializable;

/**
 * Describe customer additional details from database.
 * Is referenced by {@link Customer} class.
 *
 * @see Customer
 */
@Entity
@Table(name = "customersdetail")
public class CustomerDetail implements Serializable {
    private Long id;
    private String sex;
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
     * Return customer sex in string format.
     * For example: "male", "female", etc.
     * Use @see GenderConverter
     * to not be case sensitive.
     *
     * @return customer sex
     */
    //@Convert(converter = GenderConverter.class)
    @Column(name = "sex")
    //@Enumerated(EnumType.STRING)
    public String getSex() {
        return sex;
    }

    /**
     * Set customer sex in string format.
     * For example: "male", "female", etc.
     *
     * @param sex customer sex
     */
    public void setSex(String sex) {
        this.sex = sex;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CustomerDetail that = (CustomerDetail) o;

        if (getId() != null ? !getId().equals(that.getId()) : that.getId() != null) return false;
        if (getSex() != that.getSex()) return false;
        return getAge() != null ? getAge().equals(that.getAge()) : that.getAge() == null;
    }

    @Override
    public int hashCode() {
        int result = getId() != null ? getId().hashCode() : 0;
        result = 31 * result + (getSex() != null ? getSex().hashCode() : 0);
        result = 31 * result + (getAge() != null ? getAge().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "CustomerDetail{" +
                "id=" + id +
                ", sex=" + sex +
                ", age=" + age +
                '}';
    }
}
