-- Copyright 2018 Google Inc. All Rights Reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Schema version 2 using STRING(32) primary keys used for Hex encoded UUIDs v4;

CREATE TABLE Account (
    AccountID       STRING(32) NOT NULL,
    OldID           INT64 NOT NULL,
    Name            STRING(256) NOT NULL,
    EMail           STRING(256) NOT NULL,
    CountryCode     STRING(2) NOT NULL
) PRIMARY KEY (AccountID);

CREATE TABLE `Order` (
    OrderID         STRING(32) NOT NULL,
    OldID           INT64 NOT NULL,
    AccountID       STRING(32) NOT NULL,
    OrderDate       TIMESTAMP NOT NULL,
    Status          STRING(256) NOT NULL,
    DeliveryDate    TIMESTAMP
) PRIMARY KEY (OrderID);

CREATE TABLE OrderLineItem (
    OrderID         STRING(32) NOT NULL,
    ProductID       STRING(32) NOT NULL,
    Quantity        INT64 NOT NULL,
    Discount        INT64
) PRIMARY KEY (OrderID, ProductID);

CREATE TABLE Payment (
    OrderID         STRING(32) NOT NULL,
    PaymentID       STRING(32) NOT NULL,
    Form            STRING(32) NOT NULL,
    Status          STRING(128) NOT NULL,
    Value           FLOAT64 NOT NULL,
    Currency        STRING(3) NOT NULL,
    UpdateDate      TIMESTAMP NOT NULL
) PRIMARY KEY (OrderID, PaymentID);

CREATE TABLE Product (
    ProductID       STRING(32) NOT NULL,
    OldID           INT64 NOT NULL,
    Name            STRING(256) NOT NULL,
) PRIMARY KEY (ProductID);

CREATE TABLE ProductPrice (
    ProductID       STRING(32) NOT NULL,
    CountryCode     STRING(2) NOT NULL,
    Currency        STRING(3) NOT NULL,
    Price           FLOAT64 NOT NULL,
    ValidFromDate   TIMESTAMP NOT NULL
) PRIMARY KEY (ProductID, CountryCode, ValidFromDate);

CREATE TABLE ProductInfo (
    ProductID       STRING(32) NOT NULL,
    UpdateDate      TIMESTAMP NOT NULL,
    Data            BYTES(MAX)
) PRIMARY KEY (ProductID, UpdateDate);

CREATE TABLE ProductStock (
    ProductID       STRING(32) NOT NULL,
    LocationID      STRING(32) NOT NULL,
    Quantity        INT64 NOT NULL,
    UpdateDate      TIMESTAMP NOT NULL
) PRIMARY KEY (ProductID, LocationID, UpdateDate);

CREATE TABLE Warehouse (
    LocationID      STRING(32) NOT NULL,
    OldID           INT64 NOT NULL,
    CountryCode     STRING(2) NOT NULL,
    Name            STRING(256) NOT NULL
) PRIMARY KEY (LocationID);