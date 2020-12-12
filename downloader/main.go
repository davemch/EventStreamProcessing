// This is a small tool to download the necessary .zip-files
// from the GDELT project.
//
// Compile with `go build .` or run directly with `go run main.go`.
package main

import (
    "fmt"
    "io"
    "log"
    "net/http"
    "os"
    "time"
)

// .zip-files time frame for Black Lives Matter:
// 25.05.2020 - 25.08.2020 => 20200525_001500 - 20200825_001500
//
// The timeframe is 92 days, therefore we expect 92*24*4 = 8832 files.
//                                                     ^
//                                                     |> 4*15 min = 1 hour

func main() {
    timeStart := "20200525001500" // 25.05.2020
    timeEnd := "20200825001500"   // 25.08.2020

    now, err := parseToRFC339(timeStart)
    if err != nil {
        log.Fatal(err)
    }

    end, err := parseToRFC339(timeEnd)
    if err != nil {
        log.Fatal(err)
    }

    for !now.Equal(end) {
        // Download zip
        if err = download(parseFromRFC339(now)); err != nil {
            log.Fatal(err)
        }

        // Increase time
    	now = now.Add(time.Minute * 15) // GDELT is updated every 15 minutes
    }
}

const urlStart = "http://data.gdeltproject.org/gdeltv2/"
const urlEnd = ".export.CSV.zip"
const path = "./files/"

// download downloads the file.
func download(now string) error {
    // Create empty file
    file, err := os.Create(path + now + ".export.CSV.zip")
    if err != nil {
        return err
    }

    // Download content
    client := http.Client{}
    resp, err := client.Get(urlStart + now + urlEnd)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    // Copy content to file
    _, err = io.Copy(file, resp.Body)
    if err != nil {
        return err
    }
    defer file.Close()

    return nil
}

// parseToRFC339 parses the GDELT time format to RFC339.
func parseToRFC339(now string) (time.Time, error) {
    year := now[:4]
    month := now[4:6]
    day := now[6:8]
    hours := now[8:10]
    mins := now[10:12]

    stringRFC339 := fmt.Sprintf("%s-%s-%sT%s:%s:00Z", year, month, day, hours, mins)
    return time.Parse(time.RFC3339, stringRFC339)
}

// parseFromRFC339 parses the RFC339 time format to the GDELT time format.
func parseFromRFC339(now time.Time) string {
	nowStr := now.String()

	return fmt.Sprintf("%s%s%s%s%s00", nowStr[:4], nowStr[5:7], nowStr[8:10], nowStr[11:13], nowStr[14:16])
}
