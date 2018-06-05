package main

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"bufio"
	"strings"
	"compress/gzip"
	"time"
	"fmt"
)

var (
	startTime = time.Date(2018, 06, 04, 21, 00, 00, 00, time.UTC)
	endTime = time.Date(2018, 06, 04, 21, 30, 00, 00, time.UTC)
)

func main() {
	unfinishedJobs := forGivenFiles("/home/ubuntu/logs/2018-06-04-21.tsv.gz")
	for job,count := range countUnfinished(unfinishedJobs) {
		fmt.Printf("%s : %d\n", job, count)
	}
}

func forGivenFiles(filenames ...string) map[string]string {
	unfinishedJobs := make(map[string]string)

	for _, filename := range filenames {
		csvFile, _ := os.Open(filename)
		defer csvFile.Close()

		findUnfinished(csvFile, unfinishedJobs)
	}
	fmt.Println()

	return unfinishedJobs
}

func findUnfinished(csvFile io.Reader, unfinishedJobs map[string]string) {
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
	i := 0
	count := 0
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
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
			fmt.Printf("%s :  %d - %d\r", eventTime.String(), count, len(unfinishedJobs))
		}
		if eventTime.Before(startTime) {
			continue
		}
		if eventTime.After(endTime) {
		  if !isEnd(record[9]) {
			  continue
		  }
		} else {
		  if isStart(record[9]) {
			  count++
		  }
 		}
		handleLine(unfinishedJobs, record[9])
	}
}

func isEnd(logInformation string) bool {
	return strings.Contains(logInformation, "INFO: done")
}

func isStart(logInformation string) bool {
	return strings.Contains(logInformation, "INFO: start")
}

func handleLine(unfinishedJobs map[string]string, logMessage string) {
	sections := strings.Split(logMessage, " ")
	if isStart(logMessage) {
		unfinishedJobs[sections[4]] = sections[3]
	} else if isEnd(logMessage) {
		delete(unfinishedJobs, sections[4])
	}
}

func countUnfinished(unfinishedJobs map[string]string) map[string]int64 {
	unfinishedJobCounts := make(map[string]int64)

	for _,jobName := range unfinishedJobs {
		currentCount, ok := unfinishedJobCounts[jobName]
		if  ok {
			currentCount++
		} else {
			currentCount = 1
		}
		unfinishedJobCounts[jobName] = currentCount
	}

	return unfinishedJobCounts
}