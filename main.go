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

	weather, _ := readWeatherDataBytes(file)
	readFileTime := time.Now()
	fmt.Printf("finished reading files in %f seconds\n", readFileTime.Sub(start).Seconds())

	res := make(map[uint64]Temps)
	analyzeTime := time.Now()
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
		//fmt.Printf("adding city %s with min %f max %f avg %f\n", cities[city], t.min, t.max, t.avg)
	}

	fmt.Printf("Analyzed the data in %f seconds\n", analyzeTime.Sub(readFileTime).Seconds())
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

func readWeatherDataBytes(file io.Reader) (map[uint64][]int16, map[uint64]string) {
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	cities := make(map[uint64]string)
	weather := make(map[uint64][]int16)
	i := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		cityHash := parseCityHash(line)
		temp, err := parseTemp(line)
		if err != nil {
			panic(err.Error())
		}

		if _, exists := cities[cityHash]; !exists {
			city := parseCityName(line)
			cities[cityHash] = city
			weather[cityHash] = make([]int16, 0, 1000)
		}
		weather[cityHash] = append(weather[cityHash], temp)
		if i%100000000 == 0 {
			fmt.Printf("%v processed another 100000000 lines\n", time.Now().GoString())
		}
		i++
	}

	return weather, cities
}

func parseCityName(buffer []byte) string {
	for i, b := range buffer {
		if b == ';' {
			return string(buffer[:i])
		}
	}
	return ""
}

func parseCityHash(buffer []byte) uint64 {
	// hash := uint64(86425)
	// for _, b := range buffer {
	// 	if b == ';' {
	// 		break
	// 	}
	// 	hash += uint64(b) + hash + hash<<5
	// }
	// return hash
	return uint64(86421)
}

func parseTemp(buffer []byte) (int16, error) {
	isNegative := false
	temp := 0
	skip := true
	for _, b := range buffer {
		if skip && b != ';' {
			continue
		}
		if skip && b == ';' {
			skip = false
			continue
		}

		if b == '-' {
			isNegative = true
			continue
		}
		if b == '.' {
			continue
		}
		if b < '0' || b > '9' {
			return int16(0), fmt.Errorf("Invalid byte %c in buffer %s", b, string(buffer))
		}
		temp = temp*10 + int(b-'0')
	}
	if isNegative {
		temp = temp * -1
	}
	return int16(temp), nil
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
