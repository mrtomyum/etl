package main

import (
	"os"
	"encoding/csv"
	"strconv"
	"time"
	"fmt"
	"log"
	"sync"
)

type Product struct {
	PartNumber string
	UnitCost   float64
	UnitPrice  float64
}

type Order struct {
	CustomerNumber int
	PartNumber     string
	Quantity       int

	UnitCost  float64
	UnitPrice float64
}

func main() {
	start := time.Now()

	extractCh := make(chan *Order)
	transformCh := make(chan *Order)
	doneCh := make(chan bool)

	go extract(extractCh)
	go transform(extractCh, transformCh)
	go load(transformCh, doneCh)

	<- doneCh
	fmt.Println(time.Since(start))
}

func extract(ch chan *Order) {
	f, _ := os.Open("./orders.txt")
	defer f.Close()
	r := csv.NewReader(f)

	for field, err := r.Read(); err == nil; field, err = r.Read() {
		order := new(Order)
		order.CustomerNumber, _ = strconv.Atoi(field[0])
		order.PartNumber = field[1]
		order.Quantity, _ = strconv.Atoi(field[2])
		ch <- order
	}
	close(ch)
}

func transform(extractCh, transformCh chan *Order) {
	f, _ := os.Open("./productList.txt")
	defer f.Close()
	r := csv.NewReader(f)

	records, _ := r.ReadAll()
	productList := make(map[string]*Product)
	for _, field := range records {
		product := new(Product)
		product.PartNumber = field[0]
		product.UnitCost, _ = strconv.ParseFloat(field[1], 64)
		product.UnitPrice, _ = strconv.ParseFloat(field[2], 64)
		productList[product.PartNumber] = product
	}
	w := sync.WaitGroup{}
	for o := range extractCh {
		//numMessage++
		w.Add(1)
		go func(o *Order) {
			time.Sleep(3 * time.Millisecond)
			o.UnitCost = productList[o.PartNumber].UnitCost
			o.UnitPrice = productList[o.PartNumber].UnitPrice
			transformCh <- o
			w.Done()
		}(o)
	}

	w.Wait()
	log.Println("finish Transform")
	close(transformCh)
}

func load(tranformCh chan *Order, doneCh chan bool) {
	f, _ := os.Create("./dest.txt")
	defer f.Close()
	//numMessages := 0
	w := sync.WaitGroup{}
	fmt.Fprintf(f, "%20s%15s%12s%12s%15s%15s",
		"Part Number", "Quantity", "Unit Cost", "Unit Price", "Total Cost", "total Price")

	for o := range tranformCh {
		//numMessages++
		w.Add(1)
		go func(o *Order) {
			time.Sleep(1 * time.Millisecond)
			fmt.Fprintf(f, "%20s %15d %12.2f %12.2f %15.2f %15.2f\n",
				o.PartNumber, o.Quantity, o.UnitCost, o.UnitPrice,
				o.UnitCost*float64(o.Quantity),
				o.UnitPrice*float64(o.Quantity))
			//numMessages--
			w.Done()
		}(o)
	}
	w.Wait()
	doneCh <- true
}

// 4.6.1 - Initial ETL with synchronous processing
// Solution แรกที่ทำงานด้วยกระบวนการปกติแบบ Serialize โดยการอ่านไฟล์ แก้ไข และโหลดข้อมูลลงไฟล์ปลายทาง
//package main
//
//import (
//	"encoding/csv"
//	"fmt"
//	"os"
//	"strconv"
//	"time"
//)
//
//func main() {
//	start := time.Now()
//	orders := extract()
//	orders = transform(orders)
//	load(orders)
//	fmt.Println(time.Since(start))
//}
//
//type Product struct {
//	PartNumber string
//	UnitCost   float64
//	UnitPrice  float64
//}
//
//type Order struct {
//	CustomerNumber int
//	PartNumber     string
//	Quantity       int
//
//	UnitCost  float64
//	UnitPrice float64
//}
//
//func extract() []*Order {
//	result := []*Order{}
//
//	f, _ := os.Open("./orders.txt")
//	defer f.Close()
//	r := csv.NewReader(f)
//
//	for record, err := r.Read(); err == nil;record, err = r.Read() {
//		order := new(Order)
//		order.CustomerNumber, _ = strconv.Atoi(record[0])
//		order.PartNumber = record[1]
//		order.Quantity, _ = strconv.Atoi(record[2])
//		result = append(result, order)
//	}
//	return result
//}
//
//func transform(orders []*Order) []*Order {
//	f, _ := os.Open("./productList.txt")
//	defer f.Close()
//	r := csv.NewReader(f)
//
//	records, _ := r.ReadAll()
//	productList := make(map[string]*Product)
//	for _, record := range records {
//		product := new(Product)
//		product.PartNumber = record[0]
//		product.UnitCost, _ = strconv.ParseFloat(record[1], 64)
//		product.UnitPrice, _ = strconv.ParseFloat(record[2], 64)
//		productList[product.PartNumber] = product
//	}
//
//	for idx, _ := range orders {
//		time.Sleep(3 * time.Millisecond)
//		o := orders[idx]
//		o.UnitCost = productList[o.PartNumber].UnitCost
//		o.UnitPrice = productList[o.PartNumber].UnitPrice
//	}
//
//	return orders
//}
//
//func load(orders []*Order) {
//	f, _ := os.Create("./dest.txt")
//	defer f.Close()
//
//	fmt.Fprintf(f, "%20s%15s%12s%12s%15s%15s\n",
//		"Part Number", "Quantity",
//		"Unit Cost", "Unit Price",
//		"Total Cost", "Total Price")
//
//	for _, o := range orders {
//		time.Sleep(1 * time.Millisecond)
//		fmt.Fprintf(f, "%20s %15d %12.2f %12.2f %15.2f %15.2f\n",
//			o.PartNumber, o.Quantity,
//			o.UnitCost, o.UnitPrice,
//			o.UnitCost*float64(o.Quantity),
//			o.UnitPrice*float64(o.Quantity))
//	}
//}

