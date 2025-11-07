package loader

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"image/color"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
	//_ "github.com/duke-git/lancet/v2/fileutil"
)

func CsvOpened(f fyne.URIReadCloser) ([][]string, error) {
	if f == nil {
		log.Println("Cancelled")
		return nil, errors.New("CsvOpened called with nil")
	}
	defer f.Close()
	//content, err := fileutil.ReadCsvFile("./testdata/test.csv")
	return csv.NewReader(f).ReadAll()
}

type CSVLoader struct {
	Path     string
	f        *os.File
	Mu       sync.RWMutex
	Offsets  []int64          // 每行起始偏移，可能部分填充
	OffBuilt bool             // 是否已经完整构建完偏移表
	Cache    map[int][]string // 行缓存
	cap      int              // Cache cap
	edits    map[int]map[int]string
	cols     int
	rows     int
	TotalRow int // 文件总行数

	requestCh chan int // 用于按需加载的请求通道
	stopCh    chan struct{}
	ErrMsg    string // 错误信息

	CacheStart  int64   // 缓存开始位置
	CacheEnd    int64   // 缓存结束位置
	ActiveIndex int64   // 当前活跃索引
	maxMemory   int64   // 最大内存（目前控制在 maxMemory + loadRatio）
	loadRatio   float64 // 加载比例
	cleanRatio  float64 // 清理比例
}

// NewCSVLoader 异步启动偏移构建和请求处理
func NewCSVLoader(path string, cacheCap int) (*CSVLoader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	l := &CSVLoader{
		Path:      path,
		f:         f,
		Cache:     make(map[int][]string),
		cap:       cacheCap,
		edits:     make(map[int]map[int]string),
		requestCh: make(chan int, 256),
		stopCh:    make(chan struct{}),

		CacheStart: 0,
		CacheEnd:   0,
		maxMemory:  1 * 1024 * 124, // default 1mb
		loadRatio:  0.3,
		cleanRatio: 0.5,
	}
	// 初始只探测文件是否为空并保留第0行offset
	l.Offsets = append(l.Offsets, 0)
	go l.buildOffsetsAsync() // 后台完整构建偏移（非阻塞）
	go l.requestLoop()       // 处理按需请求
	//go func() {
	//	//for i := 0; i < 4; i++ {
	//	//	log.Println("OffBuilt: ", l.OffBuilt, len(l.Offsets))
	//	//	time.Sleep(time.Second)
	//	//}
	//	for {
	//		time.Sleep(1 * time.Second)
	//		buf := make([]byte, 1<<20)
	//		n := runtime.Stack(buf, true)
	//		fmt.Printf("%s\n", buf[:n])
	//	}
	//
	//}()
	return l, nil
}

// buildOffsetsAsync 后台逐行扫描并建立完整 Offsets（非阻塞）
func (l *CSVLoader) buildOffsetsAsync() {
	r := bufio.NewReader(l.f)
	var off int64 = 0
	// 如果文件已经从头读过（Offsets[0]=0），我们从现有偏移长度开始
	i := 0
	for {
		//// seek to current offset
		//l.Mu.RLock()
		//if i < len(l.Offsets) {
		//	off = l.Offsets[i]
		//	l.Mu.RUnlock()
		//	if _, err := l.f.Seek(off, io.SeekStart); err != nil {
		//		//fmt.Println("seek error:", err)
		//		log.Println("Error seeking to offset:", err)
		//		// 如果 seek 失败，退出构建
		//		break
		//	}
		//} else {
		//	l.Mu.RUnlock()
		//}
		safeRLock(l, func(loader *CSVLoader) {
			if i < len(loader.Offsets) {
				off = l.Offsets[i]
				if _, err := l.f.Seek(off, io.SeekStart); err != nil {
					log.Println("Error seeking to offset:", err)
					// 如果 seek 失败，退出构建
					return
				}
			}
		})
		log.Println("buildOffsetsAsync 读取第 ", i, " 行，偏移量：", off)

		line, err := r.ReadBytes('\n')
		if err != nil && err != io.EOF {
			break
		}
		//l.Mu.Lock()
		// append next offset
		off += int64(len(line))
		l.Offsets = append(l.Offsets, off)
		//l.Mu.Unlock()
		//safeLock(l, func(loader *CSVLoader) {
		//	off += int64(len(line))
		//	l.Offsets = append(l.Offsets, off)
		//})
		i++
		//if err == io.EOF {
		//	// 最后一行已处理，设置 rows
		//	//l.Mu.Lock()
		//	for !l.Mu.TryLockFunc() {
		//		time.Sleep(1 * time.Second)
		//	}
		//	l.rows = len(l.Offsets)
		//	l.OffBuilt = true
		//	// 尝试探测列数（第一行）
		//	if l.rows > 1 { // Offsets had an extra appended at EOF
		//		row, _ := l.readRowByOffsetNoCache(0)
		//		l.cols = len(row)
		//	}
		//	l.Mu.Unlock()
		//	break
		//}
		//safeLock(l, func(loader *CSVLoader) {
		l.rows = len(l.Offsets)
		l.OffBuilt = true
		// 尝试探测列数（第一行）
		if l.rows > 1 { // Offsets had an extra appended at EOF
			row, _ := l.readRowByOffsetNoCache(0)
			l.cols = len(row)
		}
		log.Println("buildOffsetsAsync 完成， 总行数：", l.rows, " 列数：", l.cols)
		return
		//l.Mu.Unlock()
		//return
		//})
		// 以非抢占方式慢速构建，避免大量 IO 占用
		// 在大文件上不阻塞 UI，但在短时间内仍会增长 Offsets
		//time.Sleep(0)
		safeLock(l, func(loader *CSVLoader) {})
		log.Println("buildOffsetsAsync 休眠三秒钟~")
		time.Sleep(3000 * time.Millisecond)
	}
}

func safeLock(l *CSVLoader, f func(loader *CSVLoader)) {
	for !l.Mu.TryLock() {
		time.Sleep(1 * time.Second)
	}
	defer l.Mu.Unlock()
	f(l)
}

func safeRLock(l *CSVLoader, f func(*CSVLoader)) {
	for !l.Mu.TryRLock() {
		time.Sleep(1 * time.Second)
	}
	defer l.Mu.RUnlock()
	f(l)
}

// requestLoop 处理按需读取请求：收到行号就读取并缓存，同时触发预读
func (l *CSVLoader) requestLoop() {
	for {
		select {
		case row := <-l.requestCh:
			// 读取这个行，并顺带预读后续若干行
			err := l.loadAndCache(row)
			if err != nil {
				log.Println(err)
			}
			// 预读后续若干行（异步）
			for i := 1; i <= 8; i++ {
				r2 := row + i
				go func() {
					err := l.loadAndCache(r2)
					if err != nil {
						log.Println("loadAndCache error:", err)
					}
				}()
			}
		case <-l.stopCh:
			return
		}
	}
}

// loadAndCache 实际从文件读取行并缓存（线程安全）
func (l *CSVLoader) loadAndCache(row int) error {
	// bounds check using Offsets if available
	l.Mu.RLock()
	if row < 0 {
		l.Mu.RUnlock()
		return errors.New("row negative")
	}
	// if Offsets known fully, check rows
	if l.OffBuilt && row >= l.rows {
		l.Mu.RUnlock()
		return errors.New("row out of range")
	}
	// Cache hit?
	if _, ok := l.Cache[row]; ok {
		l.Mu.RUnlock()
		return nil
	}
	l.Mu.RUnlock()

	// read by offset (may need to expand Offsets until row exists)
	l.Mu.Lock()
	// ensure Offsets up to row exist; if not, try to advance by seeking from last known offset
	for row >= len(l.Offsets) && !l.OffBuilt {
		// read next line to expand Offsets (synchronously here)
		if _, err := l.f.Seek(l.Offsets[len(l.Offsets)-1], io.SeekStart); err != nil {
			break
		}
		br := bufio.NewReader(l.f)
		line, err := br.ReadBytes('\n')
		nextOff := l.Offsets[len(l.Offsets)-1] + int64(len(line))
		l.Offsets = append(l.Offsets, nextOff)
		if err == io.EOF {
			l.OffBuilt = true
			l.rows = len(l.Offsets)
			break
		}
		if err != nil {
			l.Mu.Unlock()
			log.Println("read error:", err)
			return err
		}
	}
	l.Mu.Unlock()

	// Now read the row content
	rowVals, err := l.readRowByOffsetNoCache(row)
	if err != nil {
		log.Println("readRowByOffsetNoCache error:", err)
		return err
	}

	l.Mu.Lock()
	if len(l.Cache) >= l.cap {
		// 简单 eviction: 删除一个任意键（可以换成 LRU）
		for k := range l.Cache {
			delete(l.Cache, k)
			break
		}
	}
	l.Cache[row] = rowVals
	l.Mu.Unlock()
	return nil
}

// 根据 offset 读取并解析一行， 不操作 Cache
func (l *CSVLoader) readRowByOffsetNoCache(row int) ([]string, error) {
	l.Mu.RLock()
	if row >= len(l.Offsets) {
		l.Mu.RUnlock()
		return nil, errors.New("offset not available yet")
	}
	start := l.Offsets[row]
	var end int64
	if row+1 < len(l.Offsets) {
		end = l.Offsets[row+1]
	} else {
		end = -1
	}
	l.Mu.RUnlock()

	if _, err := l.f.Seek(start, io.SeekStart); err != nil {
		log.Println("seek error:", err)
		return nil, err
	}
	var buf bytes.Buffer
	if end >= 0 {
		n := end - start
		if n > 0 {
			b := make([]byte, n)
			_, err := io.ReadFull(l.f, b)
			if err != nil {
				log.Println("readFull error:", err)
				return nil, err
			}
			buf.Write(b)
		}
	} else {
		r := bufio.NewReader(l.f)
		line, err := r.ReadBytes('\n')
		if err != nil && err != io.EOF {
			log.Println("readBytes error:", err)
			return nil, err
		}
		buf.Write(line)
	}
	cr := csv.NewReader(bytes.NewReader(buf.Bytes()))
	cr.FieldsPerRecord = -1
	cols, err := cr.Read()
	if err != nil {
		cols = []string{strings.TrimRight(buf.String(), "\r\n")}
		log.Println(cols)
	}
	return cols, nil
}

// GetRowSync 尝试同步返回， 否则排队异步读取并返回 nil,error
func (l *CSVLoader) GetRowSync(row int) ([]string, error) {
	l.Mu.RLock()
	if r, ok := l.Cache[row]; ok {
		if ed, hah := l.edits[row]; hah {
			copyRow := make([]string, len(r))
			copy(copyRow, r)
			for c, v := range ed {
				if c < len(copyRow) {
					copyRow[c] = v
				} else {
					for i := len(copyRow); i <= c; i++ {
						copyRow = append(copyRow, "")
					}
					copyRow[c] = v
				}
			}
			l.Mu.RUnlock()
			return copyRow, nil
		}
		l.Mu.RUnlock()
		return r, nil
	}
	l.Mu.RUnlock()
	// not cached; push request and return nil
	select {
	case l.requestCh <- row:
	default:
		// channel 满则丢弃请求
	}
	return nil, errors.New("loading")
}

// SetEdit 保存编辑差分
func (l *CSVLoader) SetEdit(row, col int, val string) {
	l.Mu.Lock()
	defer l.Mu.Unlock()
	if l.edits[row] == nil {
		l.edits[row] = make(map[int]string)
	}
	l.edits[row][col] = val
}

// Close 停止后台 goroutine 并关闭文件
func (l *CSVLoader) Close() {
	close(l.stopCh)
	err := l.f.Close()
	if err != nil {
		log.Println("close error:", err)
		return
	}
}

func ExecMain(path string) *container.Scroll {
	// hold reference to table so loader callbacks can refresh
	var table *widget.Table
	loader, err := NewCSVLoader(path, 1024)
	if err != nil {
		fmt.Println("load error:", err)
		return nil
	}
	//defer func() {
	//	// 尝试关闭
	//	for range 3 {
	//		if loader.OffBuilt {
	//			time.Sleep(3 * time.Second)
	//			continue
	//		}
	//		loader.Close()
	//	}
	//
	//}()
	maxRows := 10000000
	loader.Mu.RLock()
	if loader.OffBuilt {
		maxRows = loader.rows
	}

	// 列数暂用1
	cols := 1
	createCell := func() fyne.CanvasObject {
		rect := canvas.NewRectangle(color.NRGBA{R: 255, G: 255, B: 255, A: 255})
		text := canvas.NewText("", color.Black)
		text.Alignment = fyne.TextAlignLeading
		text.TextSize = 14
		return container.NewStack(rect, text)
	}

	updateCell := func(id widget.TableCellID, obj fyne.CanvasObject) {
		cont := obj.(*fyne.Container)
		rect := cont.Objects[0].(*canvas.Rectangle)
		text := cont.Objects[1].(*canvas.Text)

		// background
		rect.FillColor = color.NRGBA{R: 255, G: 255, B: 255, A: 255}

		// try sync get
		rowData, err := loader.GetRowSync(id.Row)
		if err == nil && id.Col < len(rowData) {
			text.Text = rowData[id.Col]
		} else {
			text.Text = "…" // 占位 loading 标记
			// 启动异步请求（如果没有发出）
			loader.requestCh <- id.Row
			// 当行加载完成，后台会触发表格刷新：我们用 goroutine 轮询检测并在主线程刷新
			go func(r int) {
				// 等待直到行被缓存或超时
				//t0 := time.Now()
				//for {
				//	//time.Sleep(30 * time.Millisecond)
				//	time.Sleep(3000 * time.Millisecond)
				//	loader.Mu.RLock()
				//	_, ok := loader.Cache[r]
				//	loader.Mu.RUnlock()
				//	if ok || time.Since(t0) > 3*time.Second {
				//		// 在主线程刷新表格单元格
				//		fyne.Do(func() {
				//			if table != nil {
				//				table.Refresh()
				//			}
				//		})
				//		return
				//	}
				//}
				for {
					t0 := time.Now()
					//for !loader.Mu.TryRLock() {
					//	log.Println("updateCell 未获取到锁，两秒后重试~")
					//	time.Sleep(3000 * time.Millisecond)
					//}
					_, ok := loader.Cache[r]
					//loader.Mu.RUnlock()
					if ok || time.Since(t0) > 3*time.Second {
						fyne.Do(func() {
							if table != nil {
								table.Refresh()
							}
						})
						return
					}
				}

			}(id.Row)
		}
		text.Refresh()
		rect.Refresh()
	}

	table = widget.NewTable(
		func() (int, int) {
			//loader.Mu.RLock()
			//if loader.OffBuilt {
			//	r := loader.rows
			//	loader.Mu.RUnlock()
			//	loader.Mu.RLock()
			//	c := loader.cols
			//	loader.Mu.RUnlock()
			//	if c > 0 {
			//		cols = c
			//	}
			//	return r, cols
			//}
			//loader.Mu.RUnlock()
			//return maxRows, cols
			//for !loader.Mu.TryLockFunc() {
			//	log.Println("table NewTable 未获取到锁，两秒后重试~")
			//	time.Sleep(3 * time.Second)
			//}
			//defer loader.Mu.Unlock()
			if loader.OffBuilt {
				r := loader.rows
				c := loader.cols
				if c > 0 {
					cols = c
				}
				return r, cols
			}
			return maxRows, maxRows
		},
		createCell,
		updateCell,
	)

	for i := range cols {
		table.SetColumnWidth(i, 150)
		table.SetRowHeight(i, 28)
	}

	scroll := container.NewScroll(table)
	scroll.SetMinSize(fyne.Size{Width: 900, Height: 600})

	go func() {
		for {
			//loader.Mu.RLock()
			if loader.OffBuilt {
				//loader.Mu.RUnlock()
				break
			}
			//loader.Mu.RUnlock()
			//time.Sleep(1000 * time.Millisecond)
			time.Sleep(2000 * time.Millisecond)
		}
		fyne.Do(func() {
			table.Refresh()
		})
	}()

	log.Println("root set content: >>>>>>>>>>>>>> ")
	//w.SetContent(scroll)
	return scroll
}
