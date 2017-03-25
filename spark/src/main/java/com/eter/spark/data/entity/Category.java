package com.eter.spark.data.entity;

import javax.persistence.*;

/**
 * Describe products categories from database.
 *
 * @see Product
 */
@Entity
@Table(name = "categories")
public class Category {
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

}