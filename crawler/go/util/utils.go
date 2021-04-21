package util

import (
	"github.com/gocolly/colly"
	"github.com/guilherme-b/manifold/scraper/go/common"

	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"
)

func InitializeCollector(domains ...string) *colly.Collector {
	// instantiate main JSON collector
	c := colly.NewCollector(
		colly.AllowedDomains(domains...),
		colly.Async(false),
		colly.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36"),
		colly.MaxDepth(2),
	)

	// increase the timeout to 250 from the default 10 seconds
	c.SetRequestTimeout(250 * time.Second)

	c.Limit(&colly.LimitRule{
		Parallelism: 5,
		Delay:       1 * time.Second,
		RandomDelay: 5 * time.Second,
	})

	return c
}

func WriteJSON(data []common.Listing, filePath string) {
	file, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		log.Println("Unable to create json file")
		return
	}

	_ = ioutil.WriteFile(filePath, file, 0644)
}

func StartLog(logDir string) {
	f, err := os.OpenFile(logDir, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	wrt := io.MultiWriter(os.Stdout, f)
	log.SetOutput(wrt)
}
