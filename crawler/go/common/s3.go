package common

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3Handler struct {
	Session *session.Session
}

func (h *S3Handler) parseCredentials() *aws.Config {

	AccessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	SecretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	Region := os.Getenv("AWS_REGION")

	return &aws.Config{
		Region: aws.String(Region),
		Credentials: credentials.NewStaticCredentials(
		 AccessKeyID,
		 SecretAccessKey,
		 "", // a token will be generated when the session is used
		),
	   }
}

func (h *S3Handler) StartSession() error {

	config := h.parseCredentials()

	sess, err := session.NewSession(config)
	
	if err != nil {
		log.Fatalln("S3Handler: cannot construct new session,  err: %", err)
		return err
	}

	h.Session = sess

	return err
}

func (h *S3Handler) UploadFile(destinationBucket string , sourceFileName string, destinationFileName string) error {
	file, err := os.Open(sourceFileName)
	defer file.Close()

	if err != nil {
		log.Println("os.Open - filename: %, err: %", sourceFileName, err)
		return err
	}


	uploader := s3manager.NewUploader(h.Session)


	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(destinationBucket),
		ACL:    aws.String("private"),
		Key:    aws.String(destinationFileName),
		Body:   file,
	   })

	if err != nil {
		log.Println("ofck: err: %", err)
		return err
	}

	return err
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
