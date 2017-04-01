package com.eter.spark.data.entity;

import javax.persistence.*;
import java.io.Serializable;

/**
 * Describe products categories from database.
 *
 * @see Product
 */
@Entity
@Table(name = "categories")
public class Category implements Serializable {
    private Long id;
    private String name;
    private String description;

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
     * Set unique identifier for entity.
     * Is not recommended for use.
     *
     * @param id unique identifier
     */
    public void setId(Long id) {
        this.id = id;
    }

    /**
     * Return category name.
     *
     * @return category name
     */
    @Column(name = "name")
    public String getName() {
        return name;
    }

    /**
     * Set category name.
     *
     * @param name category name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Return category description.
     *
     * @return category description
     */
    @Column(name = "description")
    public String getDescription() {
        return description;
    }

    /**
     * Set category description.
     *
     * @param description category description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Category category = (Category) o;

        if (getId() != null ? !getId().equals(category.getId()) : category.getId() != null) return false;
        if (getName() != null ? !getName().equals(category.getName()) : category.getName() != null) return false;
        return getDescription() != null ? getDescription().equals(category.getDescription()) : category.getDescription() == null;
    }

    @Override
    public int hashCode() {
        int result = getId() != null ? getId().hashCode() : 0;
        result = 31 * result + (getName() != null ? getName().hashCode() : 0);
        result = 31 * result + (getDescription() != null ? getDescription().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Category{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}