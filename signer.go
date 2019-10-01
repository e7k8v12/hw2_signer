package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var debug bool = false
var mu = &sync.Mutex{}

func main() {}

func ExecutePipeline(jobs ...job) {
	in := make(chan interface{}, 100)
	var out chan interface{}
	wg := &sync.WaitGroup{}
	for _, fun := range jobs {
		out = make(chan interface{}, 100)
		wg.Add(1)
		go func(fun job, in, out chan interface{}) {
			fun(in, out)
			close(out)
			wg.Done()
		}(fun, in, out)
		in = out
	}
	wg.Wait()
}

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	for data := range in {
		wg.Add(1)
		go func(data interface{}, out chan interface{}) {
			strData := fmt.Sprintf("%v", data)
			doDebug("%v SingleHash data %v\n", strData, strData)
			chCrc32_1 := getChCRc32(strData)
			mu.Lock()
			dataMd5 := DataSignerMd5(strData)
			mu.Unlock()
			doDebug("%v SingleHash md5(data) %v\n", strData, dataMd5)
			chCrc32_2 := getChCRc32(dataMd5)
			res := <-chCrc32_1 + "~" + <-chCrc32_2
			doDebug("%v SingleHash result %v\n", strData, res)
			out <- res
			wg.Done()
		}(data, out)
	}
	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	for data := range in {
		wg.Add(1)
		go func(data interface{}, out chan interface{}) {
			strData := fmt.Sprintf("%v", data)
			chSlice := make([]chan string, 6)
			for th := 0; th < 6; th++ {
				chSlice[th] = getChCRc32(strconv.Itoa(th) + strData)
			}
			resSlice := make([]string, 6)
			for th := 0; th < 6; th++ {
				resSlice[th] = <-chSlice[th]
			}
			res := strings.Join(resSlice, "")
			out <- res
			doDebug("%v MultiHash result %v\n", strData, res)
			wg.Done()
		}(data, out)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var inSlice []string
	for val := range in {
		inSlice = append(inSlice, fmt.Sprintf("%v", val))
	}
	sort.Strings(inSlice)
	out <- strings.Join(inSlice, "_")
}

func doDebug(mess string, args ...interface{}) {
	if debug {
		fmt.Printf(mess, args...)
	}
}

func getChCRc32(data string) chan string {
	ch := make(chan string)
	go func(data string, ch chan string) {
		ch <- DataSignerCrc32(data)
	}(data, ch)
	return ch
}
