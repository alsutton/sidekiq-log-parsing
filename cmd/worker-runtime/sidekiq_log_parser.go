package main

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"bufio"
	"strings"
	"strconv"
	"fmt"
	"compress/gzip"
	"path/filepath"
)

type timingInformation struct {
	count int64
	maxtime int64
	totaltime int64
}

func main() {
	timings := make(map[string]timingInformation)

	pathS, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	filepath.Walk(pathS, func(path string, f os.FileInfo, _ error) error {
		if !f.IsDir() {
			csvFile, _ := os.Open(f.Name())
			defer csvFile.Close()

			addTimingsFromFile(csvFile, timings)
		}
		return nil
	})

	for k,v := range timings {
		fmt.Printf("%s,%d,%d,%d,%d\n", k, v.count, v.totaltime / v.count, v.maxtime, v.totaltime)
	}
}

func addTimingsFromFile(csvFile io.Reader, timings map[string]timingInformation) {
	unzippedContent, err := gzip.NewReader(csvFile)
	defer unzippedContent.Close()
	if err != nil {
		log.Fatal(err)
		return
	}

	csvReader := csv.NewReader(bufio.NewReader(unzippedContent));
	csvReader.Comma = '\t'
	csvReader.LazyQuotes = true
	csvReader.Read() // Dump the header
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break;
		}
		if err != nil {
			log.Fatal(err)
		}
		if record[4] != "production/orderweb" {
			continue;
		}
		if strings.Contains(record[9], "INFO: done:") {
			recordTiming(timings, record[9])
		}
	}
}

func recordTiming(timings map[string]timingInformation, logLine string) {
	elements := strings.Split(logLine, " ")
	timeSpent, _ := strconv.ParseFloat(elements[7], 64)
	runtime := int64(timeSpent*1000)
	timing, ok := timings[elements[3]]
	if ok == true {
		timing.count++;
		timing.totaltime += runtime
		if runtime > timing.maxtime {
			timing.maxtime = runtime
		}
		timings[elements[3]] = timing
	} else {
		timings[elements[3]] = timingInformation{1, runtime,runtime}
	}
}