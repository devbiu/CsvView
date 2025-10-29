package shower

import (
	"log"
	"math"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
	"github.com/devbiu/CsvView/loader"
)

type VirtualTable struct {
	Table       *widget.Table
	loader      *loader.CSVLoader
	Scroll      *container.Scroll
	visibleRows int
	startRow    int
	totalRows   int
}

func NewVirtualTable(l *loader.CSVLoader) *VirtualTable {
	vt := &VirtualTable{
		loader:      l,
		visibleRows: 100,
		totalRows:   len(l.Cache),
	}
	vt.Table = widget.NewTable(
		func() (int, int) {
			// TODO 根据实际需要返回总行数和列数
			return vt.totalRows, len(l.Cache[0])
		},

		func() fyne.CanvasObject {
			label := widget.NewLabel("")
			label.Truncation = fyne.TextTruncateEllipsis
			label.Selectable = true
			return label
		},

		func(id widget.TableCellID, obj fyne.CanvasObject) {
			lbl := obj.(*widget.Label)

			//if id.Row < vt.startRow || id.Row >= vt.startRow+vt.visibleRows {
			//	lbl.SetText("") // 不在可见范围内，设置为空
			//	return
			//}
			// 加载实际内容
			row := vt.startRow + (id.Row - vt.startRow)
			data := l.GetRowV2(row)
			if id.Col < len(data) && data != nil {
				lbl.SetText(data[id.Col])
			} else {
				lbl.SetText("loading...")
			}

		},
	)

	// 设置列宽
	setWidthInd := int(math.Min(float64(len(l.Cache)), 2))
	for i := range len(l.Cache[setWidthInd]) {
		vt.Table.SetColumnWidth(i, float32(len(l.Cache[setWidthInd][i])*8)+50)
	}

	// 监听滚动事件，更新 startRow
	vt.Scroll = container.NewScroll(container.NewStack(vt.Table))
	vt.Scroll.Direction = fyne.ScrollNone
	//vt.Scroll.OnScrolled = func(offset fyne.Position) {
	//	log.Printf("y: %f \n", offset.Y)
	//}
	vt.Scroll.OnScrolled = nil

	go vt.onScrollListener()
	return vt
}

func (vt *VirtualTable) onScroll(offset fyne.Position) {
	log.Printf("y: %f \n", offset.Y)
	// 获取滚动条进度
	progress := offset.Y / vt.Table.MinSize().Height
	if progress > 0.8 {
		vt.startRow += vt.visibleRows
		if vt.startRow+vt.visibleRows > vt.totalRows {
			vt.startRow = vt.totalRows - vt.visibleRows
		}
		vt.Table.Refresh()
	}

	if progress < 0.2 && vt.startRow > 0 {
		vt.startRow -= vt.visibleRows
		if vt.startRow < 0 {
			vt.startRow = 0
		}
		vt.Table.Refresh()
	}
}

func onScrollListener(sr *container.Scroll) {
	log.Println("start scroll listener")
	prev := fyne.NewPos(0, 0)
	for {
		pos := sr.Offset
		if pos != prev {
			log.Printf("y: %f \n", pos)
			prev = pos
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (vt *VirtualTable) onScrollListener() {
	log.Println("start scroll listener")
	prev := fyne.NewPos(0, 0)
	for {
		pos := vt.Scroll.Offset
		if pos != prev {
			log.Printf("y: %f \n", pos)
			prev = pos
		}
		time.Sleep(time.Second)
	}
}
