package scraper

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/guilherme-b/manifold/scraper/go/common"
	"github.com/guilherme-b/manifold/scraper/go/util"

	"github.com/gocolly/colly"
)

type PagingInformation struct {
	CurrentPage int `json:"CurrentPage"`
	TotalPages  int `json:"TotalPages"`
}

const (
	crawlerName = "pt_century21"
	websiteAddr = "https://www.century21.pt"
	apiEndpoint = "/umbraco/Surface/C21PropertiesSearchListingSurface/GetAllSEO"
	params      = `?ord=date-desc&page=pageNumber&numberOfElements=12&ba=&be=&map=&mip=&q=&v=c&ptd=&pstld=&mySite=False&masterId=1&seoId=&nodeId=46530&language=pt-PT&agencyId=&triggerbyAddressLocationLevelddl=false&AgencySite_showAllAgenciesProperties=false&AgencyExternalName=&b=2&pt=&ls=&vt=&pstl=&cc=&et=`

	// specifies the recurrence of progress log
	progressFreq = 5 * time.Second
)

var listings []common.Listing

func generateLink(pageNumber int) string {
	var formattedParams string = strings.Replace(params, "pageNumber", strconv.Itoa(pageNumber), 1)

	// the final generated link
	var link string = websiteAddr + apiEndpoint + formattedParams

	return link
}

func ListingsCount() {
	log.Println("Scraped listings: ", len(listings))
}

func parseAdministrativeData(listing *common.Listing) [3]string {
	var adminData [3]string

	// consider all text up util a parentheses is found
	r, _ := regexp.Compile(`^[^\(]+`)

	tokens := strings.Split(listing.District, ",")
	length := len(tokens)

	if length > 1 {
		listing.District = r.FindString(tokens[1])
	}
	if length > 2 {
		listing.County = r.FindString(tokens[2])
	}
	if length > 3 {
		listing.Parish = r.FindString(tokens[3])
	}

	return adminData
}

func Scrape(scrapeDate time.Time) (string, []common.Listing) {

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Exiting crawler")
		}
	}()

	var startPage int = 1

	listings = []common.Listing{}

	// instantiate main JSON collector
	c := util.InitializeCollector("century21.pt", "www.century21.pt")

	// Listing detail collector
	detailCollector := c.Clone()

	c.OnRequest(func(r *colly.Request) {
		//log.Println("Visiting: ", r.URL.String())

	})

	detailCollector.OnRequest(func(r *colly.Request) {
		//log.Println("Visiting Detail: ", r.URL.String())

	})

	c.OnError(func(r *colly.Response, e error) {
		log.Println("Error:", e, r.Request.URL, string(r.Body))
	})

	detailCollector.OnError(func(r *colly.Response, e error) {
		log.Println("Error:", e, r.Request.URL, string(r.Body))
	})

	c.OnResponse(func(r *colly.Response) {
		if strings.Index(r.Headers.Get("Content-Type"), "json") == -1 {
			return
		}

		// if the response is empty, return
		if reflect.ValueOf(r.Body).IsZero() {
			return
		}

		data := &common.ListingJSON{}
		err := json.Unmarshal(r.Body, data)

		if err != nil {
			return
		}

		// if the API returned proprieties proceed
		if len(data.Properties) > 0 {
			for _, listing := range data.Properties {
				listing.CrawledAt = time.Now()
				// parse and associate the administrative data from the concatenated Location string
				parseAdministrativeData(&listing)

				// add the listing
				listings = append(listings, listing)

				// visit the Listing's detail page
				var listingLink string = websiteAddr + "/" + listing.Url

				// pass the context containing the *Listing object
				r.Ctx.Put("listing", &listings[len(listings)-1])
				detailCollector.Request("GET", listingLink, nil, r.Ctx, nil)

				// visit the next API listings
				pagingData := &PagingInformation{}
				err := json.Unmarshal(r.Body, pagingData)

				if err == nil {
					if pagingData.CurrentPage < pagingData.TotalPages {
						var startLink string = generateLink(pagingData.CurrentPage + 1)
						c.Visit(startLink)
					}
				}

			}
		}
	})

	// parses the ammenities list
	detailCollector.OnHTML("ul.tags-list", func(e *colly.HTMLElement) {
		context := e.Response.Ctx
		listing := context.GetAny("listing")

		// if the listing is not found, return
		if listing == nil {
			log.Fatal("Failed to retrieve listing.")
			return
		}

		// get each ammenity and append it
		e.ForEach("li", func(_ int, i *colly.HTMLElement) {
			listing.(*common.Listing).AddAmmenity(i.Text)
		})
	})

	// parses the property's details (price, areas, etc)
	detailCollector.OnHTML("ul.caret-list.multi-columns", func(e *colly.HTMLElement) {
		context := e.Response.Ctx
		listing := context.GetAny("listing")

		// if the listing is not found, return
		if listing == nil {
			log.Fatal("Failed to retrieve listing.")
			return
		}

		// cast listing
		listingPointer := listing.(*common.Listing)

		// iterate through the <li> values
		e.ForEach("li", func(_ int, i *colly.HTMLElement) {
			textValue := i.Text
			// parse the price tag
			if strings.Contains(textValue, "Preço") {
				priceTag := strings.ReplaceAll(i.ChildText("strong"), " ", "")
				listingPointer.ParsePrice(&priceTag)
			} else if strings.Contains(textValue, "Certificado energético") {
				listingPointer.EnergyCertificate = i.ChildText("strong")
			} else if strings.Contains(textValue, "Tipo de Estacionamento") {
				// parse the parking space number
				// e.g. Tipo de estacionamento:&nbsp; <strong>1&nbsp;Box Fechada</strong>
				// we then need to use regex to extract the number values and sum them
				parkingTag := i.ChildText(("strong"))
				re := regexp.MustCompile(`\d`)
				matches := re.FindAll([]byte(parkingTag), -1)

				for parkingNum := range matches {
					listingPointer.ParkingSpaces += int(parkingNum)
				}
			}
		})
	})

	var startLink string = generateLink(startPage)

	logTimer := common.LogTimer{}
	logTimer.Start(progressFreq, ListingsCount)

	c.Visit(startLink)
	c.Wait()

	logTimer.Stop()

	return crawlerName, listings
}
