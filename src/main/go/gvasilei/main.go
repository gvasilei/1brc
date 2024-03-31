package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
)

type TemperatureStats struct {
	min   float64
	max   float64
	sum   float64
	count int
	avg   float64
}

func (stats TemperatureStats) String() string {
	return fmt.Sprintf("Min: %.2f, Max: %.2f, Avg: %.2f", stats.min, stats.max, stats.avg)
}

func temperatureStatsWorker(id int, lines <-chan string, results chan<- map[string]TemperatureStats, wg *sync.WaitGroup) {
	defer wg.Done()

	stats := make(map[string]TemperatureStats)

	for line := range lines {
		parts := strings.Split(line, ";")
		city := parts[0]
		temperature, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			fmt.Println("Error: ", err)
			return
		}

		recording, exists := stats[city]
		if !exists {
			stats[city] = TemperatureStats{math.Inf(1), math.Inf(-1), temperature, 1, 0.0}
		} else {
			if temperature < recording.min {
				recording.min = temperature
			} else if temperature > recording.max {
				recording.max = temperature
			}
			recording.sum += temperature
			recording.count++

			stats[city] = recording
		}

		//fmt.Printf("Worker %d finished task\n", id)
	}

	results <- stats
}

var traceFile = flag.String("trace", "", "write trace execution to `file`")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

func main() {
	log.Println("Starting the application...")
	flag.Parse()

	if *traceFile != "" {
		f, err := os.Create("./profiles/" + *traceFile)
		if err != nil {
			log.Fatal("Failed to create trace profile: ", err)
		}
		defer f.Close()
		err = trace.Start(f)
		if err != nil {
			log.Fatal("Failed to start trace: ", err)
		}
		defer trace.Stop()
	}

	if *cpuprofile != "" {
		f, err := os.Create("./profiles/" + *cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	calculate()

	if *memprofile != "" {
		f, err := os.Create("./profiles/" + *memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}

func calculate() {
	file, err := os.Open("../../../../measurements.txt")
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	stats := make(map[string]TemperatureStats)

	var wg sync.WaitGroup
	numWorkers := runtime.NumCPU() - 1
	linesChannel := make(chan string, 1000)
	results := make(chan map[string]TemperatureStats, numWorkers)

	// Create Task Workers to distribute temperature stats calculation tasks
	log.Printf("Adding %d workers to process temperature stats\n", numWorkers)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go temperatureStatsWorker(i, linesChannel, results, &wg)
	}

	for scanner.Scan() {
		linesChannel <- scanner.Text()
	}

	close(linesChannel) // No more tasks to be added, close the channel.
	wg.Wait()
	close(results)

	log.Println("Merging results...")
	for resultsMap := range results {
		for city, recording := range resultsMap {
			existingRecording, exists := stats[city]
			if !exists {
				stats[city] = recording
			} else {
				if recording.min < existingRecording.min {
					existingRecording.min = recording.min
				}
				if recording.max > existingRecording.max {
					existingRecording.max = recording.max
				}
				existingRecording.sum += recording.sum
				existingRecording.count += recording.count
				stats[city] = existingRecording
			}
		}
	}

	log.Println("Calculating averages...")
	for city, recording := range stats {
		recording.avg = recording.sum / float64(recording.count)
		stats[city] = recording
	}
	log.Println("Outputting stats...")

	fmt.Print(stats)
}
