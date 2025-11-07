package main

import (
	"errors"
	"log"
	"math"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/storage"
	"fyne.io/fyne/v2/widget"
	"github.com/devbiu/CsvView/loader"
	"github.com/devbiu/CsvView/shower"
)

var topWindow fyne.Window

var tabs *container.DocTabs

func main() {

	a := app.NewWithID("devbiu.csvView")
	//a := app.New()
	w := a.NewWindow("CsvView")
	topWindow = w
	w.SetMainMenu(makeMenu(a, w))
	l, _ := loader.NewCsvLoaderV2("./data/test_student.csv", 1024)
	table := shower.NewVirtualTable(l)
	w.SetContent(table.Scroll)
	w.Resize(fyne.NewSize(1000, 700))

	d := fyne.CurrentApp().NewWindow("debugWindow")
	d.SetContent(shower.DebugTable(l))
	d.Resize(fyne.NewSize(500, 100))
	d.Show()

	w.ShowAndRun()
}

func makeMenu(a fyne.App, w fyne.Window) *fyne.MainMenu {
	file := fyne.NewMenu("File", fyne.NewMenuItem("Open CSV", func() {
		fd := dialog.NewFileOpen(func(reader fyne.URIReadCloser, err error) {
			if err != nil {
				dialog.ShowError(err, w)
				return
			}
			if reader == nil {
				log.Println("Cancelled")
				return
			}
			csvData, err := loader.CsvOpened(reader)
			if err != nil {
				dialog.ShowError(err, w)
			}
			if csvData == nil {
				dialog.ShowError(errors.New("csv file empty"), w)
			} else if len(csvData) == 0 || len(csvData[0]) == 0 {
				dialog.ShowError(errors.New("file format is incorrect "), w)
			} else {
				showCsvWindow(a, w, reader.URI().Name(), csvData)
			}
		}, w)
		fd.SetFilter(storage.NewExtensionFileFilter([]string{".csv"}))
		fd.Show()
	}))

	showAbout := func() {
		w := a.NewWindow("About")
		w.SetContent(widget.NewLabel("About Fyne Demo app..."))
		w.Show()
	}
	aboutItem := fyne.NewMenuItem("About", showAbout)

	file.Items = append(file.Items, aboutItem)

	main := fyne.NewMainMenu(
		file,
	)
	return main
}

func showCsvWindow(a fyne.App, w fyne.Window, lebName string, csvData [][]string) {
	if tabs == nil {
		tabs = container.NewDocTabs(container.NewTabItem(lebName, ShowCsvTab(w, csvData)))
	} else {
		tabs.Append(container.NewTabItem(lebName, ShowCsvTab(w, csvData)))
	}

	w.SetContent(tabs)
	//max := container.NewMax(tabs)
	//w.SetContent(max)
}

func ShowCsvTab(w fyne.Window, data [][]string) fyne.CanvasObject {
	table := widget.NewTableWithHeaders(
		func() (int, int) { return len(data), len(data[0]) + 1 },
		func() fyne.CanvasObject {
			label := widget.NewLabel("Cell 000, 000")
			label.Truncation = fyne.TextTruncateEllipsis
			label.Selectable = true
			return label
		},
		func(id widget.TableCellID, cell fyne.CanvasObject) {
			label := cell.(*widget.Label)
			//label := cell.(*widget.Entry)
			switch id.Col {
			case len(data[id.Col]):
				label.SetText("")
			default:
				label.SetText(data[id.Row][id.Col])
			}
		})

	setWidthInd := int(math.Min(float64(len(data)), 2))
	for i := range len(data[setWidthInd]) {
		table.SetColumnWidth(i, float32(len(data[setWidthInd][i])*8)+50)
	}

	return table

}
