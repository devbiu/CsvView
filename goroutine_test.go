package main_test

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

var rw sync.RWMutex

var wg sync.WaitGroup

type testStruct struct {
	a   int
	b   string
	com bool
}

func Test1(t *testing.T) {
	fmt.Println("start!")
	ts := testStruct{a: 10, b: "example", com: false}
	for i := range 21 {
		wg.Add(1)
		go func(inx int) {
			defer wg.Done()
			defer rw.Unlock()
			//rw.Lock()
			for !rw.TryLock() {
				fmt.Println("未获取到锁，两秒后重试~")
				time.Sleep(2 * time.Second)

			}
			ts.a = inx + ts.a
			ts.b = fmt.Sprintf("value_%d", inx)

			if inx == 20 {
				ts.com = true
			}

			//rw.Unlock()
		}(i)
	}
	wg.Wait()
	rw.Lock()
	fmt.Println(ts)
	rw.Unlock()
	fmt.Println("end~")
}

func updateStruct(ts *testStruct, inx int) {
	rw.Lock()
	ts.a = inx + ts.a
	ts.b = fmt.Sprintf("value_%d", inx)

	if inx == 20 {
		ts.com = true
	}
	rw.Unlock()
}
