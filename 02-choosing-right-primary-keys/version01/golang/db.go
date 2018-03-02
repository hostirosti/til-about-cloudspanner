/*
Copyright 2018 Google Inc. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/google/uuid"
	"github.com/icrowley/fake"
	"github.com/pariz/gountries"
	"google.golang.org/api/option"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc/codes"
)

var (
	queryCountries = gountries.New()
	countries      = func() []gountries.Country {
		var cs []gountries.Country
		for _, c := range queryCountries.Countries {
			cs = append(cs, c)
		}
		return cs
	}()

	tables = []string{
		"Account",
		"Order",
		"OrderLineItem",
		"Payment",
		"Product",
		"ProductInfo",
		"ProductPrice",
		"ProductStock",
		"Warehouse",
	}
)

type blobField []byte

// Convert the blobField as CSV string
func (b *blobField) MarshalCSV() (string, error) {
	return base64.StdEncoding.EncodeToString(*b), nil
}

// Convert the CSV string as blobField
func (b *blobField) UnmarshalCSV(csv string) (err error) {
	*b, err = base64.StdEncoding.DecodeString(csv)
	if err != nil {
		return err
	}
	return nil
}

type dbAccount struct {
	AccountID   int64  `spanner:"AccountID" csv:"AccountID"`
	Name        string `spanner:"Name" csv:"Name"`
	EMail       string `spanner:"EMail" csv:"EMail"`
	CountryCode string `spanner:"CountryCode" csv:"CountryCode"`
}

func NewGeneratedAccount(id int64) *dbAccount {
	return &dbAccount{
		AccountID:   id,
		Name:        fake.FullName(),
		EMail:       fake.EmailAddress(),
		CountryCode: countries[rand.Intn(len(countries))].Alpha2,
	}
}

type dbOrder struct {
	OrderID      int64     `spanner:"OrderID" csv:"OrderID"`
	AccountID    int64     `spanner:"AccountID" csv:"AccountID"`
	OrderDate    time.Time `spanner:"OrderDate" csv:"OrderDate"`
	Status       string    `spanner:"Status" csv:"Status"`
	DeliveryDate time.Time `spanner:"DeliveryDate" csv:"DeliveryDate"`
}

var orderStatus = []string{"Ordered", "Paid", "Distributed", "Delivered"}

func NewGeneratedOrder(id int64, aID int64) *dbOrder {
	var d time.Time
	od := randomDate()
	s := orderStatus[rand.Intn(len(orderStatus))]
	if s == "Delivered" {
		d = od.Add(14 * 24 * time.Hour)
	}
	return &dbOrder{
		OrderID:      id,
		AccountID:    aID,
		OrderDate:    od,
		Status:       s,
		DeliveryDate: d,
	}
}

type dbOrderLineItem struct {
	OrderID   int64 `spanner:"OrderID" csv:"OrderID"`
	ProductID int64 `spanner:"ProductID" csv:"ProductID"`
	Quantity  int64 `spanner:"Quantity" csv:"Quantity"`
	Discount  int64 `spanner:"Discount" csv:"Discount"`
}

func NewGeneratedOrderLineItem(oID, pID int64) *dbOrderLineItem {
	return &dbOrderLineItem{
		OrderID:   oID,
		ProductID: pID,
		Quantity:  rand.Int63n(10) + int64(1),
		Discount:  rand.Int63n(15) + int64(5),
	}
}

type dbPayment struct {
	OrderID    int64     `spanner:"OrderID" csv:"OrderID"`
	PaymentID  string    `spanner:"PaymentID" csv:"PaymentID"`
	Form       string    `spanner:"Form" csv:"Form"`
	Status     string    `spanner:"Status" csv:"Status"`
	Value      float64   `spanner:"Value" csv:"Value"`
	Currency   string    `spanner:"Currency" csv:"Currency"`
	UpdateDate time.Time `spanner:"UpdateDate" csv:"UpdateDate"`
}

var paymentStatus = []string{"Rejected", "In Process", "Accepted"}

func NewGeneratedPayment(oID int64) *dbPayment {
	pid := uuid.New()
	return &dbPayment{
		OrderID:    oID,
		PaymentID:  string(encodeHexUUID(&pid)),
		Form:       fake.CreditCardType(),
		Status:     paymentStatus[rand.Intn(len(paymentStatus))],
		Value:      float64(rand.Intn(10000)) * rand.Float64(),
		Currency:   fake.CurrencyCode(),
		UpdateDate: randomDate(),
	}
}

type dbProduct struct {
	ProductID int64  `spanner:"ProductID" csv:"ProductID"`
	Name      string `spanner:"Name" csv:"Name"`
}

func NewGeneratedProduct(id int64) *dbProduct {
	return &dbProduct{
		ProductID: id,
		Name:      fake.ProductName(),
	}
}

type dbProductPrice struct {
	ProductID     int64     `spanner:"ProductID" csv:"ProductID"`
	CountryCode   string    `spanner:"CountryCode" csv:"CountryCode"`
	Currency      string    `spanner:"Currency" csv:"Currency"`
	Price         float64   `spanner:"Price" csv:"Price"`
	ValidFromDate time.Time `spanner:"ValidFromDate" csv:"ValidFromDate"`
}

func NewGeneratedProductPrice(pID int64) *dbProductPrice {
	c := countries[rand.Intn(len(countries))]
	for {
		if len(c.Currencies) > 0 {
			break
		}
		c = countries[rand.Intn(len(countries))]
	}

	return &dbProductPrice{
		ProductID:     pID,
		CountryCode:   c.Alpha2,
		Currency:      c.Currencies[rand.Intn(len(c.Currencies))],
		Price:         float64(rand.Intn(10000)) * rand.Float64(),
		ValidFromDate: randomDate(),
	}
}

type dbProductStock struct {
	ProductID  int64     `spanner:"ProductID" csv:"ProductID"`
	LocationID int64     `spanner:"LocationID" csv:"LocationID"`
	Quantity   int64     `spanner:"Quantity" csv:"Quantity"`
	UpdateDate time.Time `spanner:"UpdateDate" csv:"UpdateDate"`
}

func NewGeneratedProductStock(pID, lID int64) *dbProductStock {
	return &dbProductStock{
		ProductID:  pID,
		LocationID: lID,
		Quantity:   rand.Int63n(10000),
		UpdateDate: randomDate(),
	}
}

type dbProductInfo struct {
	ProductID  int64     `spanner:"ProductID" csv:"ProductID"`
	UpdateDate time.Time `spanner:"UpdateDate" csv:"UpdateDate"`
	Data       string    `spanner:"Data" csv:"Data"`
}

func NewGeneratedProductInfo(pID int64, size int) *dbProductInfo {
	s := make([]byte, size)
	rand.Read(s)
	return &dbProductInfo{
		ProductID:  pID,
		UpdateDate: randomDate(),
		Data:       base64.StdEncoding.EncodeToString(s),
	}
}

type dbWarehouse struct {
	LocationID  int64  `spanner:"LocationID" csv:"LocationID"`
	CountryCode string `spanner:"CountryCode" csv:"CountryCode"`
	Name        string `spanner:"Name" csv:"Name"`
}

func NewGeneratedWarehouse(id int64) *dbWarehouse {
	return &dbWarehouse{
		LocationID:  id,
		CountryCode: countries[rand.Intn(len(countries))].Alpha2,
		Name:        fake.Company(),
	}
}

// CloudSpannerService is an wrapping object of dbpath and an authenticated
// Cloud Spanner client
type CloudSpannerService struct {
	*spanner.Client
	aClient *database.DatabaseAdminClient
	iPath   string
	dbPath  string
}

// NewCloudSpannerService creates new authenticated Cloud Spanner client with the given
// service account. If no service account is provided, the default auth context is used.
func NewCloudSpannerService(svcAccJSON, project, instance, db string) (*CloudSpannerService, error) {
	iPath := fmt.Sprintf("projects/%v/instances/%v",
		strings.TrimSpace(project),
		strings.TrimSpace(instance),
	)
	dbPath := fmt.Sprintf("%v/databases/%v",
		iPath,
		strings.TrimSpace(db),
	)
	debugLog.Printf("Connecting Spanner client to %s\n", dbPath)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var opts []option.ClientOption
	if _, err := os.Stat(svcAccJSON); err == nil {
		opts = append(opts, option.WithServiceAccountFile(svcAccJSON))
		debugLog.Print("Found service account file... using for auth")
	}
	c, err := spanner.NewClientWithConfig(ctx, dbPath, spanner.ClientConfig{NumChannels: 20}, opts...)
	if err != nil {
		return nil, err
	}
	ac, err := database.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return &CloudSpannerService{c, ac, iPath, dbPath}, nil

}

func (svc *CloudSpannerService) cleanup() {
	debugLog.Println("Closing Google Cloud Spanner connections...")
	svc.Close()
	svc.aClient.Close()
	debugLog.Println("Finished closing Google Cloud Spanner connections.")
}

func (svc *CloudSpannerService) createDB(name, schema string) error {
	stmtsRaw := strings.Split(schema, ";")
	stmts := make([]string, 0, len(stmtsRaw))
	for i := 0; i < len(stmtsRaw); i++ {
		stmt := strings.TrimSpace(stmtsRaw[i])
		if len(stmt) > 0 && !strings.HasPrefix(stmt, "--") {
			stmts = append(stmts, stmt)
		}
	}
	fmt.Printf("stmts: %v\n", stmts)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	op, err := svc.aClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          svc.iPath,
		CreateStatement: "CREATE DATABASE `" + name + "`",
		ExtraStatements: stmts,
	})
	if err != nil {
		return err
	}
	_, err = op.Wait(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (svc *CloudSpannerService) updateSchema(schema string) error {
	stmtsRaw := strings.Split(schema, ";")
	stmts := make([]string, 0, len(stmtsRaw))
	for i := 0; i < len(stmtsRaw); i++ {
		stmt := strings.TrimSpace(stmtsRaw[i])
		if len(stmt) > 0 && !strings.HasPrefix(stmt, "--") {
			stmts = append(stmts, stmt)
		}
	}
	fmt.Printf("stmts: %v\n", stmts)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	op, err := svc.aClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   svc.dbPath,
		Statements: stmts,
	})
	if err != nil {
		return err
	}
	err = op.Wait(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (svc *CloudSpannerService) clearTable(table string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	_, err := svc.Apply(ctx, []*spanner.Mutation{
		spanner.Delete(table, spanner.AllKeys()),
	})
	return err
}

func appendMutation(ch chan<- *spanner.Mutation, table string, s interface{}) error {
	im, err := spanner.InsertOrUpdateStruct(table, s)
	if err != nil {
		return err
	}
	ch <- im
	return nil
}

type job struct {
	ms         []*spanner.Mutation
	retryCount int
}

type insertJobsRunner struct {
	svc         *CloudSpannerService
	parallelism int
	timeout     int
	donec       chan struct{}

	jobs      chan *job
	retry     chan *job
	jobsWG    *sync.WaitGroup
	workersWG *sync.WaitGroup
}

func (ijr *insertJobsRunner) worker() {
	ijr.workersWG.Add(1)
	for {
		select {
		case <-ijr.done():
			debugLog.Println("Exiting worker...")
			ijr.workersWG.Done()
			return
		// retry jobs if job failed before, prioritized
		case j := <-ijr.retry:
			ijr.applyMutations(j)
		case j := <-ijr.jobs:
			// retry jobs prioritized
			for len(ijr.retry) > 0 {
				select {
				case <-ijr.done():
					debugLog.Println("Exiting worker...")
					ijr.workersWG.Done()
					return
				default:
					ijr.applyMutations(<-ijr.retry)
				}
			}
			ijr.applyMutations(j)
		}
	}
}

func (ijr *insertJobsRunner) run(ch <-chan *spanner.Mutation) {
	ijr.donec = make(chan struct{})
	ijr.jobs = make(chan *job, ijr.parallelism*2)
	ijr.retry = make(chan *job, ijr.parallelism*10)
	ijr.jobsWG = &sync.WaitGroup{}
	ijr.workersWG = &sync.WaitGroup{}
	for s := 0; s < ijr.parallelism; s++ {
		go ijr.worker()
	}
	go func() {
		for m := range ch {
			ijr.jobs <- &job{
				ms: []*spanner.Mutation{m},
			}
			ijr.jobsWG.Add(1)
		}
		debugLog.Printf("Remaining jobs in channels jobs: %v and retry: %v ", len(ijr.jobs), len(ijr.retry))
		ijr.jobsWG.Wait()
		close(ijr.donec)
		debugLog.Print("Finished all jobs")
	}()
}

func (ijr *insertJobsRunner) done() <-chan struct{} {
	return ijr.donec
}

func (ijr *insertJobsRunner) stop() {
	close(ijr.donec)
}

func (ijr *insertJobsRunner) applyMutations(j *job) {
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(ijr.timeout)*time.Second)
	defer cancel()

	_, err := ijr.svc.Apply(ctx, j.ms, spanner.ApplyAtLeastOnce())

	if err == nil {
		ijr.jobsWG.Done()
		return
	}

	debugLog.Printf("Error inserting records, retrying: %s", err.Error())
	if spanner.ErrCode(err) == codes.DeadlineExceeded ||
		spanner.ErrCode(err) == codes.Canceled {
		select {
		case ijr.retry <- j:
		default:
			debugLog.Printf("Retry jobs channel is full, discarding retry: %v", err)
			ijr.jobsWG.Done()
		}
		// Slow down push on Cloud Spanner a bit
		time.Sleep(time.Duration(rand.Intn(250)) * time.Millisecond)
		return
	}

	debugLog.Printf("Error inserting records, retrying: %s", err.Error())
	if spanner.ErrCode(err) != codes.DeadlineExceeded {
		j.retryCount++
	}

	if j.retryCount > 10 {
		debugLog.Printf("Tried 10 times to persist without success. Last err: %v", err)
		ijr.jobsWG.Done()
		return
	}

	select {
	case ijr.retry <- j:
	default:
		debugLog.Printf("Retry jobs channel is full, discarding retry: %v", err)
		ijr.jobsWG.Done()
	}
}
