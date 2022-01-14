package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/jtenos/azureblobinfogo/blobs"
	"gopkg.in/yaml.v2"
)

var cfg = &Config{}

func main() {

	if !loadConfig("config.yaml") {
		if !loadConfig("secrets.yaml") {
			log.Fatalln("Error loading configuration - must be in config.yaml or secrets.yaml")
		}
	}

	w, err := os.Create("blobs.csv")
	if err != nil {
		log.Fatalln(err)
	}

	wbas, err := os.Create("blobs-basic.csv")
	if err != nil {
		log.Fatalln(err)
	}

	cw := csv.NewWriter(w)
	cwbas := csv.NewWriter(wbas)
	writeAllBlobs(cw, cwbas)
}

func loadConfig(filenm string) bool {
	file, err := os.Open(filenm)
	if file != nil {
		defer file.Close()
	}

	if err != nil {
		log.Println(err)
		return false
	}

	contents, err := ioutil.ReadAll(file)
	if err != nil {
		log.Println(err)
		return false
	}

	err = yaml.Unmarshal(contents, &cfg)
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}

func writeAllBlobs(cw *csv.Writer, cwbas *csv.Writer) {
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

	cw.Write(blobs.HeaderFields)
	cwbas.Write(blobs.BasicHeaderFields)
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
			cw.Write(rec.GetFields())
			cwbas.Write(rec.GetBasicFields())
		}
		fmt.Println("Loop complete, flushing")
		cw.Flush()
		cwbas.Flush()
		fmt.Println("Done flushing")
	}
}
