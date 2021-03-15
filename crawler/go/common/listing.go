package common

import 
(
	"time"
	"regexp"
	"fmt"
)


type ListingJSON struct {
	SearchedLocation               string    `json:"SearchedLocation"`
	ReferenceId                    string    `json:"ReferenceId"`
	PropertiesPerPage              int       `json:"PropertiesPerPage"`
	PropertiesCount                int       `json:"PropertiesCount"`
	CurrentPage                    int       `json:"CurrentPage"`
	TotalPages                     int       `json:"TotalPages"`
	LevelSearch                    string    `json:"LevelSearch"`
	LocationDisplayFilteredbyLevel string    `json:"LocationDisplayFilteredbyLevel"`
	Properties                     []Listing `json:"Properties"`
}

type Listing struct {
	ID                string `json:"ContractNumber"`
	Name              string `json:"Title"`
	Description       string `json:"Description"`
	Summary           string
	IsSold            string `json:"Sold"`
	CrawledAt         time.Time
	Price             string `json:"PriceCurrencyFormated"`
	PropertyType      string `json:"PropertyType"`
	Latitude          string `json:"Latitude"`
	Longitude         string `json:"Longitude"`
	Url               string `json:"URLSEOv2"`
	PhotoUrl          string `json:"Photo"`
	District          string `json:"FullLocation"`
	County            string
	Parish            string
	Bedrooms          string `json:"Bedrooms"`
	Bathrooms         string `json:"Bathrooms"`
	GrossArea         string `json:"AreaGross"`
	NetArea           string `json:"AreaNet"`
	EnergyCertificate string
	ParkingSpaces     int
	Ammenities        []string
}

func (listing *Listing) AddAmmenity(ammenity string) []string {
	listing.Ammenities = append(listing.Ammenities, ammenity)

	return listing.Ammenities
}


func (listing *Listing) ParsePrice(price *string) {
	r := regexp.MustCompile(`[^,€$£]*`)

	newPrices := r.FindStringSubmatch(*price)

	if len(newPrices) > 1 {
		*price = newPrices[1]
	} else {
		*price = "Unknown"
	}

	listing.Price = *price
}

func (listing *Listing) String() {
	fmt.Printf("%+v\n", listing) 
}