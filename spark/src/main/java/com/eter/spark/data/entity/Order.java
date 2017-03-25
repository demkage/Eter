package com.eter.spark.data.entity;

import javax.persistence.*;

/**
 * Describe customer orders what makes customer.
 * Exist an one-to-one relation to {@link Customer} and {@link OrderDetail}.
 *
 * @see Customer
 * @see OrderDetail
 */
@Entity
@Table(name = "orders")
public class Order {
    private Long id;
    private Customer customer;
    private OrderDetail orderDetail;

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
     * Return reference to customer.
     * Exist an one-to-one relation between {@link Order} and {@link Customer}
     * references by customerId.
     *
     * @return reference to prodcut category
     */
    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "customerId", referencedColumnName = "id")
    public Customer getCustomer() {
        return customer;
    }

    /**
     * Set customer reference.
     *
     * @param customer customer reference
     */
    public void setCustomer(Customer customer) {
        this.customer = customer;
    }


    /**
     * Return reference to order detail.
     * Exist an one-to-one relation between {@link Order} and {@link OrderDetail}
     * references by orderDetailId.
     *
     * @return reference to prodcut category
     */
    @OneToOne(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    @JoinColumn(name = "orderDetailId", referencedColumnName = "id")
    public OrderDetail getOrderDetail() {
        if (orderDetail == null)
            return new OrderDetail();

        return orderDetail;
    }

    /**
     * Set order detail reference.
     *
     * @param orderDetail order detail reference
     */
    public void setOrderDetail(OrderDetail orderDetail) {
        this.orderDetail = orderDetail;
    }
}
