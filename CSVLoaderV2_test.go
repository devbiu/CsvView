package main

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/devbiu/CsvView/loader"
)

var wg sync.WaitGroup

func TestFunc(t *testing.T) {
	l, err := loader.NewCsvLoaderV2("C:\\Users\\lishibo\\Desktop\\test_student.csv", 1024)
	if err != nil {
		t.Fatal(err)
	}

	wg.Add(1)
	go ListenCsvLoader(l)
	wg.Wait()
	for i := range 10 {
		for j := range l.Cache[i] {
			fmt.Printf("%s\t|\t", l.Cache[i][j])
		}
		fmt.Println()
	}
}

func ListenCsvLoader(l *loader.CSVLoader) {
	defer wg.Done()
	for {
		off := l.TryLockFunc(func() bool {
			if l.ErrMsg != "" {
				log.Println(l.ErrMsg)
				return true
			}
			log.Printf("cache len: %d\n", len(l.Cache))
			if l.OffBuilt {
				log.Printf("cache off: %d\n", len(l.Cache))
				return true
			}
			return false
		})
		time.Sleep(1 * time.Second)
		if off {
			break
		}

	}
}
