package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/jtenos/azureblobinfo/blobs"
	"gopkg.in/yaml.v2"
)

var cfg = &Config{}

func main() {

	loadConfig()

	w, err := os.Create("blobs.csv")
	if err != nil {
		log.Fatalln(err)
	}

	cw := csv.NewWriter(w)
	// writeCSVHeader(cw)
	// records := []blobs.BlobCsvRecord{
	// 	{
	// 		Name:         "the name",
	// 		StorageClass: "the class",
	// 		Size:         2134,
	// 		UploadDate:   time.Now(),
	// 	},
	// 	{
	// 		Name:         "another, the name",
	// 		StorageClass: "another\nclass",
	// 		Size:         3456,
	// 		UploadDate:   time.Now(),
	// 	},
	// }
	// for _, record := range records {
	// 	writeCSV(cw, &record)
	// }
	showAllBlobs(cw)
}

func loadConfig() {
	file, err := os.Open("config.yaml")
	if file != nil {
		defer file.Close()
	}

	if err != nil {
		log.Fatalf("Error opening config file: %v\n", err)
	}

	contents, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Error reading config file: %v\n", err)
	}

	err = yaml.Unmarshal(contents, &cfg)
	if err != nil {
		log.Fatalf("Error unmarshalling config file: %v\n", err)
	}
}

func writeCSVHeader(cw *csv.Writer) {
	cw.Write(blobs.HeaderFields)
}

func writeCSV(cw *csv.Writer, record *blobs.BlobCsvRecord) {
	cw.Write(record.GetFields())
}

func showAllBlobs(cw *csv.Writer) {
	cred, err := azblob.NewSharedKeyCredential(cfg.AccountName, cfg.AccountKey)
	if err != nil {
		log.Fatalf("Error creating credential: %v\n", err)
	}

	url := fmt.Sprintf("https://%v.blob.core.windows.net/", cfg.AccountName)
	serviceClient, err := azblob.NewServiceClientWithSharedKey(url, cred, nil)
	if err != nil {
		log.Fatalf("Error creating service client: %v\n", err)
	}
	container := serviceClient.NewContainerClient(cfg.ContainerName)

	pager := container.ListBlobsFlat(nil)

	writeCSVHeader(cw)
	ctx := context.Background()

	for {

		nxtPg := pager.NextPage(ctx)
		fmt.Printf("nxtPg=%v\n", nxtPg)
		if !nxtPg {
			break
		}

		if err = pager.Err(); err != nil {
			log.Fatalf("Error in pager: %v\n", err)
		}

		fmt.Print("Calling PageResponse...")
		resp := pager.PageResponse()
		fmt.Println("Done")

		fmt.Println("Starting loop")
		for _, v := range resp.ContainerListBlobFlatSegmentResult.Segment.BlobItems {
			rec := blobs.BlobCsvRecord{
				Name:         *v.Name,
				StorageClass: string(*v.Properties.BlobType),
				Size:         *v.Properties.ContentLength,
				UploadDate:   *v.Properties.CreationTime,
			}
			writeCSV(cw, &rec)
		}
		fmt.Println("Loop complete, flushing")
		cw.Flush()
		fmt.Println("Done flushing")
	}
}
