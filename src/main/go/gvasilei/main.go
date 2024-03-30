package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"runtime/trace"
	"strconv"
	"strings"
)

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

	type TemperatureStats struct {
		mix float64
		max float64
		avg float64
	}

	scanner := bufio.NewScanner(file)
	measurements := make(map[string][]float64)
	stats := make(map[string]TemperatureStats)

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

	for city, temps := range measurements {

		min := math.Inf(1)
		max := math.Inf(-1)
		sum := 0.0

		for _, temp := range temps {
			sum += temp
			if temp < min {
				min = temp
			}
			if temp > max {
				max = temp
			}
		}
		avg := math.Round(sum / float64(len(temps)))
		stats[city] = TemperatureStats{min, max, avg}
	}

	//for city := range stats {
	//	fmt.Printf("City: %s, Min: %.2f, Max: %.2f, Avg: %.2f\n", city, stats[city].mix, stats[city].max, stats[city].avg)
	//}
	log.Println("Outputting stats...")
	fmt.Print(stats)
}
