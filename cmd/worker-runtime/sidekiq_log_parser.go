package main

import (
	"os"
	"fmt"
	"time"
	"strings"
	"strconv"
	"io"
	"compress/gzip"
	"log"
	"encoding/csv"
	"bufio"
)

type timingInformation struct {
	count int64
	maxtime int64
	totaltime int64
}

var (
	startTime = time.Date(2018, 06,01, 10, 28, 00, 00, time.UTC)
	endTime = time.Date(2018, 06, 01, 10, 30, 00, 00, time.UTC)
)

func main() {
	timings := make(map[string]timingInformation)

	forGivenFiles(timings, "/home/ubuntu/logs/2018-06-01-10.tsv.gz")

	for k,v := range timings {
		fmt.Printf("%s,%d,%d,%d,%d\n", k, v.count, v.totaltime / v.count, v.maxtime, v.totaltime)
	}
}

func forGivenFiles(timings map[string]timingInformation, filenames ...string) {
	for _, filename := range filenames {
		csvFile, _ := os.Open(filename)
		defer csvFile.Close()

		addTimingsFromFile(csvFile, timings)
	}
}

/*
func forAllFilesInThisDirectory(timings map[string]timingInformation) {
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
}
*/

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
	i := 0
	count := 0
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break;
		}
		if err != nil {
			log.Fatal(err)
		}
		if record[4] != "production/orderweb" {
			continue
		}
		eventTime, err := time.Parse(time.RFC3339, record[2])
		if err != nil {
			log.Fatal(err)
			return
		}
		i++
		if i >= 1000 {
			fmt.Printf("%s :  %d", eventTime.String(), count)
		}
		if eventTime.Before(startTime) || eventTime.After(endTime) {
			continue
		}
		if strings.Contains(record[9], "INFO: done:") {
			recordTiming(timings, record[9])
			count++
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