package com.eter.spark.data.entity;


import scala.Serializable;

import javax.persistence.*;


/**
 * Describe product information from database.
 * Exist one-to-one relation between {@link Product} and {@link Category}
 *
 * @see Category
 */
@Entity
@Table(name = "products")
public class Product implements Serializable {

    private Long id;
    private String name;
    private String description;
    private Double price;
    private Integer stock;
    private Double discount;
    private Category category;

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
     * Return product name.
     *
     * @return product name
     */
    @Column(name = "name")
    public String getName() {
        return name;
    }

    /**
     * Set product name.
     *
     * @param name product name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Return product description.
     *
     * @return product description
     */
    @Column(name = "description")
    public String getDescription() {
        return description;
    }

    /**
     * Set product description.
     *
     * @param description product description
     */
    public void setDescription(String description) {
        this.description = description;
    }


    /**
     * Return product price.
     *
     * @return product price
     */
    @Column(name = "price")
    public Double getPrice() {
        return price;
    }

    /**
     * Set product price.
     *
     * @param price product price
     */
    public void setPrice(double price) {
        this.price = price;
    }

    /**
     * Return product count available in stock.
     *
     * @return product count in stock
     */
    @Column(name = "stock")
    public Integer getStock() {
        return stock;
    }

    /**
     * Set product count available in stock.
     *
     * @param stock product count in stock
     */
    public void setStock(Integer stock) {
        this.stock = stock;
    }


    /**
     * Return product discount.
     * Recommended value between 0 and 1.
     *
     * @return product discount
     */
    @Column(name = "discount")
    public Double getDiscount() {
        return discount;
    }


    /**
     * Set product discount value.
     * Recommended value between 0 and 1.
     *
     * @param discount product discount
     */
    public void setDiscount(Double discount) {
        this.discount = discount;
    }

    /**
     * Return reference to product category.
     * Exist an one-to-one relation between {@link Product} and {@link Category}
     * references by categoryId.
     *
     * @return reference to prodcut category
     */
    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "categoryId", referencedColumnName = "id")
    public Category getCategory() {
        return category;
    }

    /**
     * Set reference to product category.
     *
     * @param category reference to product category
     */
    public void setCategory(Category category) {
        this.category = category;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Product product = (Product) o;

        if (getId() != null ? !getId().equals(product.getId()) : product.getId() != null) return false;
        if (getName() != null ? !getName().equals(product.getName()) : product.getName() != null) return false;
        if (getDescription() != null ? !getDescription().equals(product.getDescription()) : product.getDescription() != null)
            return false;
        if (getPrice() != null ? !getPrice().equals(product.getPrice()) : product.getPrice() != null) return false;
        if (getStock() != null ? !getStock().equals(product.getStock()) : product.getStock() != null) return false;
        if (getDiscount() != null ? !getDiscount().equals(product.getDiscount()) : product.getDiscount() != null)
            return false;
        return getCategory() != null ? getCategory().equals(product.getCategory()) : product.getCategory() == null;
    }

    @Override
    public int hashCode() {
        int result = getId() != null ? getId().hashCode() : 0;
        result = 31 * result + (getName() != null ? getName().hashCode() : 0);
        result = 31 * result + (getDescription() != null ? getDescription().hashCode() : 0);
        result = 31 * result + (getPrice() != null ? getPrice().hashCode() : 0);
        result = 31 * result + (getStock() != null ? getStock().hashCode() : 0);
        result = 31 * result + (getDiscount() != null ? getDiscount().hashCode() : 0);
        result = 31 * result + (getCategory() != null ? getCategory().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Product{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", price=" + price +
                ", stock=" + stock +
                ", discount=" + discount +
                ", category=" + category +
                '}';
    }
}