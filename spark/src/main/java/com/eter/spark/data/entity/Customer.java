package com.eter.spark.data.entity;

import javax.persistence.*;
import java.io.Serializable;

/**
 * Describe customer base information from database.
 * Exist an one-to-one relation between this class and {@link CustomerDetail}.
 *
 * @see CustomerDetail
 */
@Entity
@Table(name = "customers")
public class Customer implements Serializable {
    private Long id;
    private String firstName;
    private String lastName;
    private String email;
    private String address;
    private String city;
    private String postalCode;
    private CustomerDetail customerDetail;


    /**
     * Return unique identifier of entity.
     *
     * @return unique identifier
     */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false)
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
     * Return customer first name.
     *
     * @return customer first name
     */
    @Column(name = "firstName")
    public String getFirstName() {
        return firstName;
    }

    /**
     * Set customer first name.
     *
     * @param firstName customer first name
     */
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    /**
     * Return customer last name.
     *
     * @return customer last name
     */

    @Column(name = "lastName")
    public String getLastName() {
        return lastName;
    }

    /**
     * Set customer first name.
     *
     * @param lastName customer last name
     */
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    /**
     * Return customer email.
     *
     * @return customer email
     */
    @Column(name = "email")
    public String getEmail() {
        return email;
    }

    /**
     * Set customer email.
     *
     * @param email customer email
     */
    public void setEmail(String email) {
        this.email = email;
    }

    /**
     * Return customer living address.
     *
     * @return customer living address
     */
    @Column(name = "address")
    public String getAddress() {
        return address;
    }

    /**
     * Set customer living address.
     *
     * @param address customer living address
     */
    public void setAddress(String address) {
        this.address = address;
    }

    /**
     * Return customer register city.
     *
     * @return customer city
     */
    @Column(name = "city")
    public String getCity() {
        return city;
    }

    /**
     * Set customer city.
     *
     * @param city customer city
     */
    public void setCity(String city) {
        this.city = city;
    }

    /**
     * Return customer postal code.
     *
     * @return customer postal code
     */
    @Column(name = "postalCode")
    public String getPostalCode() {
        return postalCode;
    }

    /**
     * Set customer postal code.
     *
     * @param postalCode customer postal code
     */
    public void setPostalCode(String postalCode) {
        this.postalCode = postalCode;
    }

    /**
     * Return reference to customer detail.
     * Exist an one-to-one relation between {@link Customer} and {@link CustomerDetail}
     * references by customerDetailId.
     *
     * @return reference to customer detail
     */
    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "customerDetailId", referencedColumnName = "id", nullable = true)
    public CustomerDetail getCustomerDetail() {
        return customerDetail;
    }

    /**
     * Set reference to customer detail.
     *
     * @param customerDetail customer detail
     */
    public void setCustomerDetail(CustomerDetail customerDetail) {
        this.customerDetail = customerDetail;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Customer customer = (Customer) o;

        if (getId() != null ? !getId().equals(customer.getId()) : customer.getId() != null) return false;
        if (getFirstName() != null ? !getFirstName().equals(customer.getFirstName()) : customer.getFirstName() != null)
            return false;
        if (getLastName() != null ? !getLastName().equals(customer.getLastName()) : customer.getLastName() != null)
            return false;
        if (getEmail() != null ? !getEmail().equals(customer.getEmail()) : customer.getEmail() != null) return false;
        if (getAddress() != null ? !getAddress().equals(customer.getAddress()) : customer.getAddress() != null)
            return false;
        if (getCity() != null ? !getCity().equals(customer.getCity()) : customer.getCity() != null) return false;
        return getPostalCode() != null ? getPostalCode().equals(customer.getPostalCode()) : customer.getPostalCode() == null;
        //return getCustomerDetail() != null ? getCustomerDetail().equals(customer.getCustomerDetail()) : customer.getCustomerDetail() == null;
    }

    @Override
    public int hashCode() {
        int result = getId() != null ? getId().hashCode() : 0;
        result = 31 * result + (getFirstName() != null ? getFirstName().hashCode() : 0);
        result = 31 * result + (getLastName() != null ? getLastName().hashCode() : 0);
        result = 31 * result + (getEmail() != null ? getEmail().hashCode() : 0);
        result = 31 * result + (getAddress() != null ? getAddress().hashCode() : 0);
        result = 31 * result + (getCity() != null ? getCity().hashCode() : 0);
        result = 31 * result + (getPostalCode() != null ? getPostalCode().hashCode() : 0);
        //result = 31 * result + (getCustomerDetail() != null ? getCustomerDetail().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "id=" + id +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", email='" + email + '\'' +
                ", address='" + address + '\'' +
                ", city='" + city + '\'' +
                ", postalCode='" + postalCode + '\'' +
                //", customerDetail=" + customerDetail +
                '}';
    }
}
