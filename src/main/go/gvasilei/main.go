package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
)

type TemperatureStats struct {
	mix float64
	max float64
	avg float64
}

type CityRecording struct {
	city  string
	temps []float64
}

type CityStats struct {
	mu    sync.RWMutex
	stats map[string]TemperatureStats
}

func NewCityStats() *CityStats {
	return &CityStats{
		stats: make(map[string]TemperatureStats),
	}
}

// Set a key-value pair in the map safely.
func (sm *CityStats) Set(key string, value TemperatureStats) {
	sm.mu.Lock() // Acquire the lock for writing
	sm.stats[key] = value
	sm.mu.Unlock() // Release the lock after writing
}

// Get a value from the map safely.
func (sm *CityStats) Get(key string) (TemperatureStats, bool) {
	sm.mu.RLock() // Acquire the lock for reading
	value, exists := sm.stats[key]
	sm.mu.RUnlock() // Release the lock after reading
	return value, exists
}

func worker(id int, recordings <-chan CityRecording, wg *sync.WaitGroup, stats *CityStats) {
	defer wg.Done()
	for recording := range recordings {
		min := math.Inf(1)
		max := math.Inf(-1)
		sum := 0.0

		for _, temp := range recording.temps {
			sum += temp
			if temp < min {
				min = temp
			}
			if temp > max {
				max = temp
			}
		}
		avg := sum / float64(len(recording.temps))
		stats.Set(recording.city, TemperatureStats{min, max, avg})
		//fmt.Printf("Worker %d finished task\n", id)
	}
}

var traceFile = flag.String("trace", "", "write trace execution to `file`")

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

	file, err := os.Open("../../../../measurements.txt")
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	measurements := make(map[string][]float64)
	stats := NewCityStats()

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ";")
		city := parts[0]
		temperature, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			fmt.Println("Error: ", err)
			return
		}

		measurements[city] = append(measurements[city], temperature)

	}

	var wg sync.WaitGroup
	recordingsChannel := make(chan CityRecording, 50)

	// Create Task Workers to distribute temperature stats calculation tasks
	for i := 0; i < runtime.NumCPU()-1; i++ {
		wg.Add(1)
		go worker(i, recordingsChannel, &wg, stats)
	}

	for city, temps := range measurements {
		recordingsChannel <- CityRecording{city, temps}
	}

	close(recordingsChannel) // No more tasks to be added, close the channel.
	wg.Wait()

	//for city := range stats {
	//	fmt.Printf("City: %s, Min: %.2f, Max: %.2f, Avg: %.2f\n", city, stats[city].mix, stats[city].max, stats[city].avg)
	//}
	log.Println("Outputting stats...")
	fmt.Print(stats)
}
