package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)

type Temps struct {
	min float64
	max float64
	avg float64
}

func main() {
	start := time.Now()
	file, err := os.Open("./measurements.txt")
	if err != nil {
		panic(err.Error())
	}
	defer file.Close()

	weather := readWeatherData(file)
	readFileTime := time.Now()
	fmt.Printf("finished reading files in %f seconds\f", readFileTime.Sub(start).Seconds())

	res := make(map[string]Temps)
	for city, temps := range weather {
		var min int16 = 2555
		var max int16 = -2555
		var acc int64

		for i := 0; i < len(temps); i++ {
			if temps[i] < min {
				min = temps[i]
			}
			if temps[i] > max {
				max = temps[i]
			}
			acc += int64(temps[i])
		}
		t := Temps{float64(min / 10), float64(max / 10), float64(acc/int64(len(temps))) / 10}
		res[city] = t
		fmt.Printf("adding city %s with min %f max %f avg %f\n", city, t.min, t.max, t.avg)
	}

	end := time.Now()
	fmt.Printf("finished %d cities in %v seconds", len(res), end.Sub(start).Seconds())
}

func readWeatherData(file io.Reader) map[string][]int16 {
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	// const maxCapacity = 2048 * 1024
	// buf := make([]byte, maxCapacity)
	// scanner.Buffer(buf, maxCapacity)
	weather := make(map[string][]int16)
	for scanner.Scan() {
		line := scanner.Text()
		city, temp := getDataAsInt(line)
		if _, exists := weather[city]; exists {
			weather[city] = append(weather[city], temp)
		} else {
			weather[city] = []int16{temp}
		}
	}

	return weather
}

func getDataAsInt(entry string) (string, int16) {
	for i := 0; i < len(entry); i++ {
		if int(entry[i]) == 59 {
			v, err := strconv.ParseFloat(entry[i+1:], 64)
			if err != nil {
				panic(err.Error())
			}
			return entry[0:i], int16(v * 10)
		}
	}
	return "", 0
}
