package blobs

import (
	"strconv"
	"time"
)

type BlobCsvRecord struct {
	Name         string
	StorageClass string
	Size         int64
	UploadDate   time.Time
}

func (r *BlobCsvRecord) GetFields() []string {
	return []string{
		r.Name,
		r.StorageClass,
		strconv.FormatInt(r.Size, 10),
		r.UploadDate.Format("2006-01-02 15:04:05"),
	}
}

func (r *BlobCsvRecord) GetBasicFields() []string {
	return []string{
		r.Name,
		strconv.FormatInt(r.Size, 10),
	}
}
