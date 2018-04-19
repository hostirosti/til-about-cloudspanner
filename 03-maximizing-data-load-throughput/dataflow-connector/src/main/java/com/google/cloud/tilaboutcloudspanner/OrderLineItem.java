/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.tilaboutcloudspanner;

import java.io.Serializable;
import java.util.Objects;

public class OrderLineItem implements Serializable {

    private String orderID;
    private String productID;
    private Long quantity;
    private Long discount;

    private OrderLineItem() {

    };
    // CSV line structure OrderID,ProductID,Quantity,Discount
    public OrderLineItem(String csvLine) {
        if (csvLine == null) {
            throw new IllegalStateException("csv line is empty");
        }
        String[] elems = csvLine.split(",");
        if (elems.length != 4) {
            throw new IllegalStateException("Not enough columns in csv line to create OrderLineItem");
        }

        this.orderID = BatchImportPipeline.getBase64EncodedUUID();
        this.productID = BatchImportPipeline.getBase64EncodedUUID();
        this.quantity = Long.parseLong(elems[2]);
        this.discount = Long.parseLong(elems[3]);
    }

    public String getOrderID() {
        return orderID;
    }

    public String getProductID() {
        return productID;
    }

    public Long getQuantity() {
        return quantity;
    }

    public Long getDiscount() {
        return discount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderLineItem that = (OrderLineItem) o;
        return Objects.equals(orderID, that.orderID) &&
            Objects.equals(productID, that.productID) &&
            Objects.equals(quantity, that.quantity) &&
            Objects.equals(discount, that.discount);
    }

    @Override
    public int hashCode() {

        return Objects.hash(orderID, productID, quantity, discount);
    }
}
