package main

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"bufio"
	"strings"
	"compress/gzip"
	"path/filepath"
	"time"
	"fmt"
	"sync"
)

func main() {
	pathS, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup

	filepath.Walk(pathS, func(path string, f os.FileInfo, _ error) error {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".gz") {
			wg.Add(1)
			go func() {
				defer wg.Done()
				addTimingsFromFile(f)
			}()
		}
		return nil
	})
	wg.Wait()
}

func addTimingsFromFile(f os.FileInfo) {
	csvFile, _ := os.Open(f.Name())
	defer csvFile.Close()

	outputFilename, _ := os.Create(f.Name()+".log")
	defer outputFilename.Close()
	bufferedWriter := bufio.NewWriter(outputFilename)

	unzippedContent, err := gzip.NewReader(csvFile)
	defer unzippedContent.Close()
	if err != nil {
		log.Fatal(err)
		return
	}

	csvReader := csv.NewReader(bufio.NewReader(unzippedContent))
	csvReader.Comma = '\t'
	csvReader.LazyQuotes = true
	csvReader.Read() // Dump the header
	currentCounter := int64(0)
	currentTime := time.Now().Truncate(time.Second)
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if !strings.Contains(record[4], "production") || !strings.Contains(record[4], "orderweb") {
			continue
		}
		if !strings.Contains(record[9], "INFO: queueing") {
			continue
		}
		entryTime, err := time.Parse(time.RFC3339, record[2])
		if err != nil {
			log.Fatal(err)
		}
		entryTime.Truncate(time.Second)
		if currentTime.Equal(entryTime) {
			currentCounter++
		} else {
			if currentCounter != 0 {
				values := fmt.Sprintf("%s,%d\n", currentTime.String(), currentCounter)
				bufferedWriter.WriteString(values)
			}
			currentCounter = 1
			currentTime = entryTime
		}
	}
	bufferedWriter.Flush()
}