package main

import (
	"flag"
	"log"
	"os"
	"reflect"
	"time"

	"github.com/guilherme-b/manifold/scraper/go/common"
	"github.com/guilherme-b/manifold/scraper/go/scraper"
	"github.com/guilherme-b/manifold/scraper/go/util"
)

const bucketSignature = "{year}/{month}/{week}/{botname}.json"

// defines the scraper to be called per input string
var scraperMap = map[string]interface{}{
	"century21_pt": scraper.Scrape,
}

// initializes a scraper based on the provided name string
func startScraper(name string, args ...interface{}) (string, []common.Listing) {
	log.Println("Initializing scraper ", name, " at ", time.Now().Format("2006-01-02 15:04:05"))

	var output []common.Listing
	var botName string

	switch name {
	case "century21_pt":
		botName, output = scraperMap[name].(func(time.Time) (string, []common.Listing))(args[0].(time.Time))
	}

	return botName, output
}

func main() {
	crawler := flag.String("crawler", "", "the crawler to run")

	awsS3Bucket := flag.String("aws_s3bucket", "", "the AWS S3 bucket name")
	crawlerTime := flag.String("crawler_time", time.Now().Format("2006-01-02 15:04:05"), "The crawler's run date, will reflect on file save path")
	bucketSignature := flag.String("bucket_signature", "", "the destination bucket's template")

	flag.Parse()

	if reflect.ValueOf(*crawler).IsZero() {
		log.Println("aborting: no crawler provided")
		return
	}

	if reflect.ValueOf(*awsS3Bucket).IsZero() {
		log.Println("aborting: could not retrieve AWS S3 metadata")
		return
	}

	if _, ok := scraperMap[*crawler]; !ok {
		log.Println("Could not find scraper ", *crawler)
		return
	}

	if reflect.ValueOf(*bucketSignature).IsZero() {
		*bucketSignature = "/{year}/{month}/{week}/"
	}

	format := "2006-01-02"
	t, err := time.Parse(format, *crawlerTime)

	if err != nil {
		log.Println("Could not parse date from", *crawlerTime)
		return
	}

	// initiate the log output
	util.StartLog("./tmp/main.log")

	// start the desired scraper
	botName, listings := startScraper(*crawler, t)

	// save the output locally
	var outputPath string = "./tmp/" + botName + ".json"
	util.WriteJSON(listings, outputPath)

	// dump to S3
	if listings != nil && len(listings) > 0 {

		// the destination name is a combination between the bucket and the formatted signature
		outputPath := common.GenerateBucketName(*bucketSignature, botName, t)
		outputPath = *awsS3Bucket + outputPath
		outputFileName := botName + ".json"

		filePath := "./tmp/" + outputFileName

		s3Handler := common.S3Handler{}

		err = s3Handler.StartSession()
		err = s3Handler.UploadFile(outputPath, filePath, outputFileName)

		// if the file has succesfully been uploaded, delete the file
		if err != nil {
			os.Remove(outputPath)
		}
	}

}
