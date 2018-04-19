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
	"sort"
	"strconv"
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
	AccountID   string `spanner:"AccountID" csv:"-"`
	OldID       int64  `spanner:"OldID" csv:"AccountID"`
	Name        string `spanner:"Name" csv:"Name"`
	EMail       string `spanner:"EMail" csv:"EMail"`
	CountryCode string `spanner:"CountryCode" csv:"CountryCode"`
}

func NewGeneratedAccount(id int64) *dbAccount {
	return &dbAccount{
		OldID:       id,
		Name:        fake.FullName(),
		EMail:       fake.EmailAddress(),
		CountryCode: countries[rand.Intn(len(countries))].Alpha2,
	}
}

type dbOrder struct {
	OrderID      string    `spanner:"OrderID" csv:"-"`
	OldID        int64     `spanner:"OldID" csv:"OrderID"`
	AccountID    string    `spanner:"AccountID" csv:"AccountID"`
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
		OldID:        id,
		AccountID:    strconv.FormatInt(aID, 10),
		OrderDate:    od,
		Status:       s,
		DeliveryDate: d,
	}
}

type dbOrderLineItem struct {
	OrderID   string `spanner:"OrderID" csv:"OrderID"`
	ProductID string `spanner:"ProductID" csv:"ProductID"`
	Quantity  int64  `spanner:"Quantity" csv:"Quantity"`
	Discount  int64  `spanner:"Discount" csv:"Discount"`
}

func NewGeneratedOrderLineItem(oID, pID int64) *dbOrderLineItem {
	return &dbOrderLineItem{
		OrderID:   strconv.FormatInt(oID, 10),
		ProductID: strconv.FormatInt(pID, 10),
		Quantity:  rand.Int63n(10) + int64(1),
		Discount:  rand.Int63n(15) + int64(5),
	}
}

type dbPayment struct {
	OrderID    string    `spanner:"OrderID" csv:"OrderID"`
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
		OrderID:    strconv.FormatInt(oID, 10),
		PaymentID:  string(encodeHexUUID(&pid)),
		Form:       fake.CreditCardType(),
		Status:     paymentStatus[rand.Intn(len(paymentStatus))],
		Value:      float64(rand.Intn(10000)) * rand.Float64(),
		Currency:   fake.CurrencyCode(),
		UpdateDate: randomDate(),
	}
}

type dbProduct struct {
	ProductID string `spanner:"ProductID" csv:"-"`
	OldID     int64  `spanner:"OldID" csv:"ProductID"`
	Name      string `spanner:"Name" csv:"Name"`
}

func NewGeneratedProduct(id int64) *dbProduct {
	return &dbProduct{
		OldID: id,
		Name:  fake.ProductName(),
	}
}

type dbProductPrice struct {
	ProductID     string    `spanner:"ProductID" csv:"ProductID"`
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
		ProductID:     strconv.FormatInt(pID, 10),
		CountryCode:   c.Alpha2,
		Currency:      c.Currencies[rand.Intn(len(c.Currencies))],
		Price:         float64(rand.Intn(10000)) * rand.Float64(),
		ValidFromDate: randomDate(),
	}
}

type dbProductStock struct {
	ProductID  string    `spanner:"ProductID" csv:"ProductID"`
	LocationID string    `spanner:"LocationID" csv:"LocationID"`
	Quantity   int64     `spanner:"Quantity" csv:"Quantity"`
	UpdateDate time.Time `spanner:"UpdateDate" csv:"UpdateDate"`
}

func NewGeneratedProductStock(pID, lID int64) *dbProductStock {
	return &dbProductStock{
		ProductID:  strconv.FormatInt(pID, 10),
		LocationID: strconv.FormatInt(lID, 10),
		Quantity:   rand.Int63n(10000),
		UpdateDate: randomDate(),
	}
}

type dbProductInfo struct {
	ProductID  string    `spanner:"ProductID" csv:"ProductID"`
	UpdateDate time.Time `spanner:"UpdateDate" csv:"UpdateDate"`
	Data       string    `spanner:"Data" csv:"Data"`
}

func NewGeneratedProductInfo(pID int64, size int) *dbProductInfo {
	s := make([]byte, size)
	rand.Read(s)
	return &dbProductInfo{
		ProductID:  strconv.FormatInt(pID, 10),
		UpdateDate: randomDate(),
		Data:       base64.StdEncoding.EncodeToString(s),
	}
}

type dbWarehouse struct {
	LocationID  string `spanner:"LocationID" csv:"-"`
	OldID       int64  `spanner:"OldID" csv:"LocationID"`
	CountryCode string `spanner:"CountryCode" csv:"CountryCode"`
	Name        string `spanner:"Name" csv:"Name"`
}

func NewGeneratedWarehouse(id int64) *dbWarehouse {
	return &dbWarehouse{
		OldID:       id,
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

func (svc *CloudSpannerService) read(sql string, f func(r *spanner.Row) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeout)*time.Second)
	defer cancel()
	stmt := spanner.Statement{
		SQL: sql,
	}
	return svc.Single().Query(ctx, stmt).Do(f)
}

func (svc *CloudSpannerService) clearTable(table string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	_, err := svc.Apply(ctx, []*spanner.Mutation{
		spanner.Delete(table, spanner.AllKeys()),
	})
	return err
}

func appendMutation(ch chan<- map[string]*spanner.Mutation, table, key string, s interface{}) error {
	im, err := spanner.InsertOrUpdateStruct(table, s)
	if err != nil {
		return err
	}
	ch <- map[string]*spanner.Mutation{key: im}
	return nil
}

type buff struct {
	b       map[string][]*spanner.Mutation
	bufSize int
}

func (b *buff) sortedBatches(batchSize int) [][]*spanner.Mutation {
	if b.bufSize == 0 {
		return nil
	}

	var keys []string
	for k := range b.b {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sbs [][]*spanner.Mutation
	var ms []*spanner.Mutation
	for _, k := range keys {
		for _, m := range b.b[k] {
			ms = append(ms, m)

			if len(ms) >= batchSize {
				sbs = append(sbs, ms)
				ms = nil
			}
		}
	}

	return sbs
}

func (b *buff) add(k string, v *spanner.Mutation) {
	if _, ok := b.b[k]; ok {
		b.b[k] = append(b.b[k], v)
		return
	}
	b.b[k] = []*spanner.Mutation{v}
	b.bufSize++
	return
}

func (b *buff) reset() {
	b.b = make(map[string][]*spanner.Mutation)
	b.bufSize = 0
}

type job struct {
	ms         []*spanner.Mutation
	retryCount int
}

type insertJobsRunner struct {
	svc          *CloudSpannerService
	parallelism  int
	timeout      int
	maxBatchSize int
	donec        chan struct{}

	jobs      chan *job
	highPrio  chan *job
	lowPrio   chan *job
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
		case j := <-ijr.highPrio:
			debugLog.Printf("Pending jobs: %v highPrio, %v normal, %v lowPrio \n",
				len(ijr.highPrio), len(ijr.jobs), len(ijr.lowPrio))
			ijr.applyMutations(j)
		case j := <-ijr.jobs:
			for len(ijr.highPrio) > 0 {
				debugLog.Printf("Pending jobs: %v highPrio, %v normal, %v lowPrio \n",
					len(ijr.highPrio), len(ijr.jobs), len(ijr.lowPrio))
				select {
				case <-ijr.done():
					debugLog.Println("Exiting worker...")
					ijr.workersWG.Done()
					return
				default:
					ijr.applyMutations(<-ijr.highPrio)
				}
			}
			ijr.applyMutations(j)
			if float32(len(ijr.lowPrio))/float32(cap(ijr.lowPrio)) > 0.9 {
				ijr.applyMutations(<-ijr.lowPrio)
			}
		case j := <-ijr.lowPrio:
			for len(ijr.highPrio) > 0 || len(ijr.jobs) > 0 {
				if len(ijr.highPrio) > 0 {
					debugLog.Printf("Pending jobs: %v highPrio, %v normal, %v lowPrio \n",
						len(ijr.highPrio), len(ijr.jobs), len(ijr.lowPrio))
					select {
					case <-ijr.done():
						debugLog.Println("Exiting worker...")
						ijr.workersWG.Done()
						return
					default:
						ijr.applyMutations(<-ijr.highPrio)
					}
				}
				if len(ijr.jobs) > 0 {
					select {
					case <-ijr.done():
						debugLog.Println("Exiting worker...")
						ijr.workersWG.Done()
						return
					default:
						ijr.applyMutations(<-ijr.jobs)
					}
				}
			}
			ijr.applyMutations(j)
		}
	}
}

func (ijr *insertJobsRunner) run(chs []chan map[string]*spanner.Mutation) {
	ijr.donec = make(chan struct{})
	ijr.jobs = make(chan *job, ijr.parallelism*2)
	ijr.highPrio = make(chan *job, ijr.parallelism*2)
	ijr.lowPrio = make(chan *job, ijr.parallelism*10)
	ijr.jobsWG = &sync.WaitGroup{}
	ijr.workersWG = &sync.WaitGroup{}
	for s := 0; s < ijr.parallelism; s++ {
		go ijr.worker()
	}

	go func() {
		var wg sync.WaitGroup
		for _, ch := range chs {
			wg.Add(1)
			go func(c <-chan map[string]*spanner.Mutation, wg *sync.WaitGroup) {
				buf := &buff{
					b: make(map[string][]*spanner.Mutation),
				}
				for km := range c {
					for k, m := range km {
						buf.add(k, m)
						if buf.bufSize >= ijr.maxBatchSize*10 {
							ijr.flush(buf)
						}
					}
				}
				if buf.bufSize > 0 {
					ijr.flush(buf)
				}
				wg.Done()
				debugLog.Printf("Remaining jobs in channels jobs: %v, highPrio: %v, and lowPrio: %v ", len(ijr.jobs), len(ijr.highPrio), len(ijr.lowPrio))
			}(ch, &wg)
		}
		wg.Wait()
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

func (ijr *insertJobsRunner) flush(buf *buff) {
	for _, ms := range buf.sortedBatches(ijr.maxBatchSize) {
		ijr.jobs <- &job{ms: ms}
		ijr.jobsWG.Add(1)
	}

	buf.reset()
}

func (ijr *insertJobsRunner) applyMutations(j *job) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(ijr.timeout)*time.Second)
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
		case ijr.highPrio <- j:
		default:
			debugLog.Printf("HighPrio jobs channel is full, discarding retry: %v", err)
			ijr.jobsWG.Done()
		}
		// Slow down push on Cloud Spanner a bit
		time.Sleep(time.Duration(rand.Intn(250)) * time.Millisecond)
		return
	}

	j.retryCount++

	if j.retryCount > 10 {
		debugLog.Printf("Tried 10 times to persist without success. Last err: %v", err)
		ijr.jobsWG.Done()
		return
	}

	select {
	case ijr.lowPrio <- j:
	default:
		debugLog.Printf("LowPrio jobs channel is full, discarding retry: %v", err)
		ijr.jobsWG.Done()
	}
}
