package main

import (
	"encoding/csv"
	"log"
	"os"
	"strconv"
	"sync"
)

type CsvWriter struct {
	mutex *sync.Mutex
	csvWriter *csv.Writer
}

func NewCsvWriter(fileName string) (*CsvWriter, error) {
	csvFile, err := os.Create(fileName)
	if err != nil {
		log.Println("Create file fail with err: " + err.Error())
		return nil, err
	}

	w := csv.NewWriter(csvFile)
	return &CsvWriter{
		mutex:     &sync.Mutex{},
		csvWriter: w,
	}, nil
}

func (w *CsvWriter) Write(row []string)  {
	w.mutex.Lock()
	w.csvWriter.Write(row)
	w.mutex.Unlock()
}

func (w *CsvWriter) Flush()  {
	w.mutex.Lock()
	w.csvWriter.Flush()
	w.mutex.Unlock()
}

func main() {
	w, err := NewCsvWriter("./test.csv")
	if err != nil {
		log.Panic(err)
	}
	w.Write([]string{"id1","id2","id3"})

	count := 100000000000
	done := make(chan bool, count)

	for i := 0; i < count; i++ {
		go func(i int) {
			w.Write([]string {strconv.Itoa(i), strconv.Itoa(i), strconv.Itoa(i)})
			done <- true
		}(i)
	}

	for i:=0; i < count; i++ {
		<- done
	}
	w.Flush()
}
