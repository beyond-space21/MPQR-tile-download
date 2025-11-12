package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	jsoniter "github.com/json-iterator/go"
)

var works map[string][][]int
var client *mongo.Client
var collection *mongo.Collection
var cfg aws.Config
var totalUploads int32
var Alive_routines int32
var mutex sync.Mutex

var surveyBorder = "https://tngis.tnega.org/geoserver/wms?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&FORMAT=image%2Fpng&TRANSPARENT=true&LAYERS=cadastral_information%3Asn_wise_tn_classification&WIDTH=256&HEIGHT=256&CRS=EPSG%3A3857&STYLES=&FORMAT_OPTIONS=dpi%3A124&BBOX="

func main() {
	var err error

	// Load the JSON data
	data, err := os.ReadFile("batch_cords.json")
	if err != nil {
		panic(err)
	}
	jsonData := string(data)
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	err = json.Unmarshal([]byte(jsonData), &works)
	if err != nil {
		fmt.Println("Error unmarshalling JSON:", err)
		return
	}

	// MongoDB connection
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err = mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		panic(err)
	}
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		panic(err)
	}
	collection = client.Database("gov_").Collection("TN_inner")

	// AWS S3 configuration
	cfg, err = config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("ap-south-2"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"AKIAQWO5JSUXRPDZJGZP",
			"YuftiM2uEC/NsH9dnzTge8QYm6LR0zXuQR9WnDla",
			"",
		)),
	)
	if err != nil {
		panic(err)
	}

	// Processes
	var wg sync.WaitGroup
	for i := 1; i <= 500; i++ {
		wg.Add(1)
		go thd(&wg, i, works["b"+fmt.Sprint(i)])
	}
	wg.Wait()
}

// Routine
func thd(wg *sync.WaitGroup, batch int, work [][]int) {
	defer wg.Done()

	mutex.Lock()
	Alive_routines++
	mutex.Unlock()

	last := readCount(batch)
	l := int32(len(work))

	mutex.Lock()
	totalUploads = totalUploads + last
	mutex.Unlock()

	for i := last; i < l; i++ {
		x := work[i][0]
		y := work[i][1]

		err := toS3("https://tngis.tn.gov.in/data/xyz_tiles/cadastral_xyz/18/"+fmt.Sprint(x)+"/"+fmt.Sprint(y)+".png", "survey_subdiv_nov2025/"+fmt.Sprint(x)+"/"+fmt.Sprint(y)+".png")
		if err != nil {
			fmt.Println("Error uploading to S3:", err)
			break
		}

		mutex.Lock()
		totalUploads++
		mutex.Unlock()

		e := updateCount(batch, i)
		if e != nil {
			fmt.Println("Error updating to MDB:", err)
			break
		}

		fmt.Println(Alive_routines, totalUploads)
	}
	// fmt.Println("ended: ", batch)
	mutex.Lock()
	Alive_routines--
	mutex.Unlock()
}

func updateCount(batch int, count int32) error {
	filter := bson.D{{Key: "batch", Value: batch}}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "last", Value: count}}}}

	result, err := collection.UpdateOne(context.TODO(), filter, update)
	if err != nil {
		fmt.Println("Error updating document:", err)
		return err
	} else if result.MatchedCount == 0 {
		fmt.Println("No document matched the filter")
		return fmt.Errorf("no document matched the filter")
	}
	// else if result.ModifiedCount == 0 {
	// 	fmt.Println("Document was matched but not modified")
	// 	return fmt.Errorf("document was matched but not modified")
	// }

	return nil
}

func readCount(batch int) int32 {
	var result bson.M
	filter := bson.D{{Key: "batch", Value: batch}}

	err := collection.FindOne(context.TODO(), filter).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			collection.InsertOne(context.TODO(), bson.D{{Key: "batch", Value: batch}, {Key: "last", Value: 0}})
			fmt.Println("No document found with the given filter.")
		} else {
			panic(err)
		}
		return 0
	}
	return result["last"].(int32)
}

func toS3(downloadFrom, path string) error {

	client := &http.Client{
		Timeout: 60 * time.Second, // Adjust this as needed
	}

	var response *http.Response
	var res_er error
	for i := 0; i < 5; i++ {
		response, res_er = client.Get(downloadFrom)
		if(res_er == nil){
			break
		}

		time.Sleep(2 * time.Second)
	}
	if res_er != nil {
		return res_er
	}

	defer response.Body.Close()

	file, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}

	svc := s3.NewFromConfig(cfg)
	_, err = svc.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String("prod-assets.mypropertyqr.in"),
		Key:    aws.String(path),
		Body:   bytes.NewReader(file),
	})
	return err
}

func latLonToWebMercator(lat, lon float64) (float64, float64) {
	x := lon * 20037508.34 / 180
	y := math.Log(math.Tan((90+lat)*math.Pi/360)) / (math.Pi / 180)
	y = y * 20037508.34 / 180
	return x, y
}

func radiansToDegrees(radians float64) float64 {
	return radians * 180 / math.Pi
}

func getTileBBox(tileColumn, tileRow, zoom int) string {
	n := math.Pow(2, float64(zoom))

	lonMin := float64(tileColumn)/n*360 - 180
	latMin := radiansToDegrees(math.Atan(math.Sinh(math.Pi * (1 - 2*float64(tileRow)/n))))
	lonMax := float64(tileColumn+1)/n*360 - 180
	latMax := radiansToDegrees(math.Atan(math.Sinh(math.Pi * (1 - 2*float64(tileRow+1)/n))))

	minX, minY := latLonToWebMercator(latMin, lonMin)
	maxX, maxY := latLonToWebMercator(latMax, lonMax)

	return fmt.Sprintf("%f,%f,%f,%f", minX, maxY, maxX, minY)
}
