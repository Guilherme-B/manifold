package common

import (
	"bytes"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type AWSConfig struct {
	Bucket string
	Region string
	Key    string
	ACL    string // "public-read"
}

type S3Handler struct {
	Session *session.Session
	Config  *AWSConfig
}

func (h S3Handler) StartSession() error {
	sess, err := session.NewSession(&aws.Config{Region: aws.String(h.Config.Region)})

	if err != nil {
		log.Fatalln("S3Handler: cannot construct new session,  err: %", err)
		return err
	}

	h.Session = sess

	return nil
}

func (h S3Handler) UploadFile(fileName string) error {
	file, err := os.Open(fileName)
	defer file.Close()

	if err != nil {
		log.Println("os.Open - filename: %, err: %", fileName, err)
		return err
	}

	_, err = s3.New(h.Session).PutObject(&s3.PutObjectInput{
		Bucket:             aws.String(h.Config.Bucket),
		Key:                aws.String(h.Config.Key),
		ACL:                aws.String(h.Config.ACL),
		Body:               file,
		ContentDisposition: aws.String("attachment"),
	})

	return err
}

func (h S3Handler) ReadFile(key string) (string, error) {
	results, err := s3.New(h.Session).GetObject(&s3.GetObjectInput{
		Bucket: aws.String(h.Config.Bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return "", err
	}

	defer results.Body.Close()

	buf := bytes.NewBuffer(nil)

	if _, err := io.Copy(buf, results.Body); err != nil {
		return "", err
	}

	return string(buf.Bytes()), nil
}

func GenerateBucketName(bucketSignature string, botName string, refDate time.Time) string {
	year, week := refDate.ISOWeek()
	month := int(refDate.Month())
	day := refDate.Day()

	r := strings.NewReplacer("{year}", strconv.Itoa(year), "{month}", strconv.Itoa(month),
		"{week}", strconv.Itoa(week), "{day}", strconv.Itoa(day), "{botname}", botName)
	bucketSignature = r.Replace(bucketSignature)

	return bucketSignature
}
