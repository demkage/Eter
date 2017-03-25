package com.eter.spark.data.entity;

import javax.persistence.*;

/**
 * Describe additional order detail from database.
 * Exist an one-to-one relation between this class and {@link Product}.
 *
 * @see Order
 * @see Product
 */
@Entity
@Table(name = "ordersdetail")
public class OrderDetail {
    private Long id;
    private Product product;

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
     * Return reference to product category.
     * Exist an one-to-one relation between {@link OrderDetail} and {@link Product}
     * references by productId.
     *
     * @return reference to product
     */
    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "productId", referencedColumnName = "id")
    public Product getProduct() {
        return product;
    }

    /**
     * Set reference to product.
     *
     * @param product reference to product
     */
    public void setProduct(Product product) {
        this.product = product;
    }

}