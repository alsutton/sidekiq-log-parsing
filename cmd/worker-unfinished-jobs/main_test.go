package main

import (
	"testing"
	"strings"
)

var (
	sampleStartLine = "940429985167593485	2018-04-01T08:30:39	2018-06-04T23:00:03Z	1638666161	staging/rider-planning	34.244.22.47	Mail	Info	sidekiq/8446/86d08eac	2018-06-04T23:00:01.839Z 17 TID-gn0kpmpao GenerateZoneSlotReleasesWorker JID-f4427572ac488bf17666c6f2 INFO: start"
	sampleDoneLine = "940429985167593489	2018-04-01T08:30:40	2018-06-04T23:00:03Z	1638666161	staging/rider-planning	34.244.22.47	Mail	Info	sidekiq/8446/86d08eac	2018-06-04T23:00:01.840Z 17 TID-gn0lmgks4 GenerateZoneSlotReleasesWorker JID-f4427572ac488bf17666c6f2 INFO: done: 0.067 sec"
)

func TestIsStart(t *testing.T) {
	actualIsStart := isStart(sampleStartLine)
	if !actualIsStart {
		t.Fatal("Start was not detected")
	}
	actualIsStart = isStart(sampleDoneLine)
	if actualIsStart {
		t.Fatal("End detected as start")
	}
}

func TestIsEnd(t *testing.T) {
	actualIsEnd := isEnd(sampleStartLine)
	if actualIsEnd {
		t.Fatal("Start detected as End")
	}
	actualIsEnd = isEnd(sampleDoneLine)
	if !actualIsEnd {
		t.Fatal("End not detected")
	}
}

func TestHandleLineForStart(t *testing.T) {
	unfinishedJobs := make(map[string]string)
	startLineArray := strings.Split(sampleStartLine, "\t")
	handleLine(unfinishedJobs, startLineArray[9])
	if _, ok := unfinishedJobs["JID-f4427572ac488bf17666c6f2"]; !ok {
		t.Fatal("Job start was not detected")
	}
}

func TestHandleLineForEnd(t *testing.T) {
	unfinishedJobs := make(map[string]string)
	unfinishedJobs["JID-f4427572ac488bf17666c6f2"] = "GenerateZoneSlotReleasesWorker"
	startLineArray := strings.Split(sampleDoneLine, "\t")
	handleLine(unfinishedJobs, startLineArray[9])
	if _, ok := unfinishedJobs["JID-f4427572ac488bf17666c6f2"]; ok {
		t.Fatal("Job end was not detected")
	}
}