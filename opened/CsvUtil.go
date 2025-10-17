package opened

import (
	"encoding/csv"
	"errors"
	"log"

	"fyne.io/fyne/v2"
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
