package main

import (
	"bytes"
	"fmt"
	"math"

	// "github.com/pkg/profile"
	"os"
	"sync"
	"syscall"
)

const mb = 1024 * 1024
const filename = "./testfile"
const pageSize = mb * 128

// const filename = "./testfile_small"

type City struct {
	Name  string
	Sum   int
	Count int
	Min   int
	Max   int
}

type Cities map[string]*City

func NewCities(capacity int) *Cities {
	cities := make(Cities, capacity)
	return &cities
}

func (it *Cities) SetCity(name string, sum, count, minimum, maximum int) {
	city := (*it)[name]
	if city == nil {
		city = &City{
			Name:  name,
			Sum:   sum,
			Count: count,
			Min:   minimum,
			Max:   maximum,
		}
		(*it)[name] = city
		return
	}

	city.Sum += sum
	city.Count += count
	city.Min = min(minimum, city.Min)
	city.Max = max(maximum, city.Max)
}

func main() {
	// p := profile.Start(
	// profile.CPUProfile,
	// profile.MemProfile,
	// 	profile.ProfilePath("."),
	// 	profile.NoShutdownHook,
	// )
	// defer p.Stop()

	f, _ := os.OpenFile(filename, os.O_RDONLY, 0)
	defer f.Close()
	stat, _ := f.Stat()
	fileSize := stat.Size()

	pagesCount := math.Ceil(float64(fileSize) / float64(pageSize))
	type remaining struct {
		start []byte
		end   []byte
	}
	remainings := make([]remaining, int64(pagesCount))

	cities := NewCities(1000)
	chunkCitiesChan := make(chan *Cities, 100)

	go func() {
		for chunk := range chunkCitiesChan {
			for k, v := range *chunk {
				cities.SetCity(k, v.Sum, v.Count, v.Min, v.Max)
			}
		}
	}()

	group := sync.WaitGroup{}

	for index, offset := 0, int64(0); offset < fileSize; index, offset = index+1, offset+pageSize {
		go func(index int) {
			group.Add(1)
			defer group.Done()
			mmap := Mmap(f, offset, pageSize)

			// cut first line or whatever rest of line from previous chunk
			newlineIndexFromStart := bytes.IndexByte(mmap, '\n')
			remainings[index].start = append([]byte(nil), mmap[:newlineIndexFromStart+1]...)
			// cut whatever rest incomplete line and save for next iteration
			newlineIndexFromEnd := bytes.LastIndexByte(mmap, '\n')
			remainings[index].end = append([]byte(nil), mmap[newlineIndexFromEnd+1:]...)

			chunk := mmap[newlineIndexFromStart+1 : newlineIndexFromEnd+1]
			chunkCitiesChan <- processChunk(chunk)
		}(index)
	}
	group.Wait()
	close(chunkCitiesChan)

	remainingsBytes := make([]byte, 64*1024)
	for _, remaining := range remainings {
		remainingsBytes = append(remainingsBytes, remaining.start...)
		remainingsBytes = append(remainingsBytes, remaining.end...)
	}
	for k, v := range *processChunk(remainingsBytes) {
		cities.SetCity(k, v.Sum, v.Count, v.Min, v.Max)
	}

	for _, v := range *cities {
		fmt.Println(v.Name, "Sum:", v.Sum, "Count:", v.Count, "Min:", v.Min, "Max:", v.Max)
	}
}

type MMap []byte

func Mmap(file *os.File, offset, pageSize int64) MMap {
	if stats, _ := file.Stat(); stats.Size() < offset+pageSize {
		pageSize = stats.Size() - offset
	}

	data := make([]byte, pageSize)
	_, err := file.ReadAt(data, offset)
	if err != nil {
		panic("Cannot read file: " + err.Error())
	}
	// data, err := syscall.Mmap(
	// 	int(file.Fd()),
	// 	offset,
	// 	int(pageSize),
	// 	syscall.PROT_READ,
	// 	syscall.MAP_SHARED,
	// )
	// if err != nil {
	// 	panic("Cannot create mmap: " + err.Error())
	// }

	return MMap(data)
}

func (m MMap) Unmap() error {
	return syscall.Munmap(m)
}

func processChunk(chunk []byte) *Cities {
	cities := NewCities(1000)
	var pointer int
	for pointer < len(chunk) {

		start, semi, newline := nextline(chunk, pointer)
		city := chunk[start:semi]
		temperature := chunk[semi+1 : newline]
		pointer = newline + 1

		temp := fastParseUint(string(temperature))
		cities.SetCity(string(city), temp, 1, temp, temp)
	}
	return cities
}

func nextline(data []byte, pointer int) (startIndex, semiIndex, newlineIndex int) {
	startIndex = pointer
	for i := startIndex; i < len(data); i++ {
		if data[i] == ';' {
			semiIndex = i
			continue
		}
		if data[i] == '\n' {
			newlineIndex = i
			break
		}
	}

	return startIndex, semiIndex, newlineIndex
}

func fastParseUint(s string) int {
	if len(s) == 1 {
		ch := s[0]
		if ch < '0' || ch > '9' {
			return 0
		}
		return int(ch - '0')
	}

	return int((s[0]-'0')*10 + (s[1] - '0'))
}

