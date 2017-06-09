package main

import (
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"log"
	"net/http"

	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
	"github.com/spf13/pflag"
)

type awsHandler struct {
	AccessKey  string
	SecretKey  string
	S3Endpoint string
}

func getBucketNameFromPath(p string) string {
	if strings.HasPrefix(p, "/") {
		parts := strings.Split(p, "/")
		if len(parts) >= 2 {
			return parts[1]
		}
	}
	return ""
}

func getBucketPathFromPath(p string) string {
	if strings.HasPrefix(p, "/") {
		parts := strings.Split(p, "/")
		if len(parts) > 2 {
			ret := parts[2]
			for _, part := range parts[3:] {
				ret += "/" + part
			}
			return ret
		}
	}
	return ""
}
func (awsh *awsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("%s request %s\n", r.RemoteAddr, r.URL.Path)

	var auth aws.Auth
	auth.AccessKey = awsh.AccessKey
	auth.SecretKey = awsh.SecretKey

	var region aws.Region
	region.S3Endpoint = awsh.S3Endpoint

	client := s3.New(auth, region)
	resp, err := client.ListBuckets()

	if err != nil {
		log.Fatal(err)
		http.Error(w, fmt.Sprintf("ListBuckets err %v", err), http.StatusNotFound)
		return
	}

	if len(resp.Buckets) == 0 {
		http.Error(w, fmt.Sprintf("len(Buckets)==0"), http.StatusNotFound)
		return
	}

	bucketName, bucketPath := getBucketNameFromPath(r.URL.Path), getBucketPathFromPath(r.URL.Path)

	var bucket *s3.Bucket
	if bucketName == "" {
		w.Write([]byte(fmt.Sprintf("<body>\n")))
		for _, b := range resp.Buckets {
			w.Write([]byte(fmt.Sprintf("<br><tr>&emsp;&emsp;<a href=\"http://%s/%s\"> <font size=\"18\"> %s</font></a></tr>", r.Host, b.Name, b.Name)))
		}
		w.Write([]byte(fmt.Sprintf("</body>\n")))
		return
	} else {
		for i, b := range resp.Buckets {
			if b.Name == bucketName {
				bucket = &resp.Buckets[i]
				break
			}
		}
		if bucket == nil {
			http.Error(w, fmt.Sprintf("bucket %s is not found", bucketName), http.StatusNotFound)
			return
		}
	}

	bc, err := bucket.GetBucketContents()
	if err == nil {
		if bucketPath == "" {
			paths := make([]string, 0, len(*bc))
			for s, _ := range *bc {
				paths = append(paths, s)
			}
			sort.Strings(paths)
			w.Write([]byte(fmt.Sprintf("<body>\n")))
			for i, _ := range paths {
				w.Write([]byte(fmt.Sprintf("<br><tr>&emsp;&emsp;<a href=\"http://%s/%s/%s\"> <font size=\"18\"> %s</font></a></tr>",
					r.Host, bucket.Name, paths[len(paths)-i-1], paths[len(paths)-i-1])))
			}
			w.Write([]byte(fmt.Sprintf("</body>\n")))
			return
		} else {
			var findPath bool
			var size int64
			for s, key := range *bc {
				if path.Clean(s) == path.Clean(bucketPath) {
					findPath = true
					size = key.Size
					break
				}
			}
			if findPath == false {
				http.Error(w, fmt.Sprintf("cannot find path:%s in bucket:%s", bucketPath, bucketName), http.StatusNotFound)
				return
			}
			connectRead, err := bucket.GetReader(path.Clean(bucketPath))
			if err != nil {
				http.Error(w, fmt.Sprintf("get connectRead err:%v", err), http.StatusNotFound)
				return
			}

			w.Header().Set("Accept-Ranges", "bytes")
			if w.Header().Get("Content-Encoding") == "" {
				w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			}
			w.WriteHeader(http.StatusOK)
			io.CopyN(w, connectRead, size)
		}
	} else {
		http.Error(w, fmt.Sprintf("GetBucketContents err=%v", err), http.StatusNotFound)
		return
	}

}

func main() {
	var accessKey, secretKey, s3Endpoint, port string
	fs := pflag.CommandLine
	fs.StringVar(&accessKey, "accesskey", "", "aws accesskey")
	fs.StringVar(&secretKey, "secretkey", "", "aws secretkey")
	fs.StringVar(&s3Endpoint, "s3endpoint", "", "aws region s3endpoint")
	fs.StringVar(&port, "port", "", "httpserver listen port")
	pflag.Parse()
	awsh := &awsHandler{
		AccessKey:  accessKey,  //"QYLMTYEH6YC5VIJ04FSX",
		SecretKey:  secretKey,  // "tPiENzJDy88xawCDgYvOY84HWHpHWfoa56XZRm33",
		S3Endpoint: s3Endpoint, //"http://10.19.1.1:30150",
	}
	if accessKey == "" {
		log.Printf("accessKey should not be empty")
		os.Exit(-1)
	}
	if secretKey == "" {
		log.Printf("secretKey should not be empty")
		os.Exit(-1)
	}
	if s3Endpoint == "" {
		log.Printf("s3Endpoint should not be empty")
		os.Exit(-1)
	}
	if port == "" {
		log.Printf("port should not be empty")
		os.Exit(-1)
	}
	http.Handle("/", awsh)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		os.Exit(-1)
	}
}
