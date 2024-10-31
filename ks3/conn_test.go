package ks3

import (
	"net/http"
	"os"
	"strings"
	"time"

	. "gopkg.in/check.v1"
)

type Ks3ConnSuite struct{}

var _ = Suite(&Ks3ConnSuite{})

func (s *Ks3ConnSuite) TestURLMarker(c *C) {
	c.Skip("skip conn")
	um := UrlMaker{}
	um.Init("docs.github.com", true, false, false)
	c.Assert(um.Type, Equals, urlTypeCname)
	c.Assert(um.Scheme, Equals, "http")
	c.Assert(um.NetLoc, Equals, "docs.github.com")

	c.Assert(um.getURL("bucket", "object", "params").String(), Equals, "http://docs.github.com/object?params")
	c.Assert(um.getURL("bucket", "object", "").String(), Equals, "http://docs.github.com/object")
	c.Assert(um.getURL("", "object", "").String(), Equals, "http://docs.github.com/object")

	var conn Conn
	conn.config = getDefaultKs3Config()
	conn.config.AuthVersion = AuthV1
	c.Assert(conn.getResource("bucket", "object", "subres"), Equals, "/bucket/object?subres")
	c.Assert(conn.getResource("bucket", "object", ""), Equals, "/bucket/object")
	c.Assert(conn.getResource("", "object", ""), Equals, "/")

	um.Init("https://docs.github.com", true, false, true)
	c.Assert(um.Type, Equals, urlTypeCname)
	c.Assert(um.Scheme, Equals, "https")
	c.Assert(um.NetLoc, Equals, "docs.github.com")
	host, path := um.buildURL("bucket", "object")
	c.Assert(host, Equals, "docs.github.com")
	c.Assert(path, Equals, "/object")

	um.Init("https://docs.github.com", false, false, true)
 	c.Assert(um.Type, Equals, urlTypeksyun)
	c.Assert(um.Scheme, Equals, "https")
	c.Assert(um.NetLoc, Equals, "docs.github.com")
	host, path = um.buildURL("bucket", "object")
	c.Assert(host, Equals, "docs.github.com")
	c.Assert(path, Equals, "/bucket/object")

	um.Init("https://docs.github.com", true, false, false)
	c.Assert(um.Type, Equals, urlTypeCname)
	c.Assert(um.Scheme, Equals, "https")
	c.Assert(um.NetLoc, Equals, "docs.github.com")
	host, path = um.buildURL("bucket", "object")
	c.Assert(host, Equals, "docs.github.com")
	c.Assert(path, Equals, "/object")

	um.Init("http://docs.github.com", true, false, false)
	c.Assert(um.Type, Equals, urlTypeCname)
	c.Assert(um.Scheme, Equals, "http")
	c.Assert(um.NetLoc, Equals, "docs.github.com")

	um.Init("docs.github.com:8080", false, true, false)
	c.Assert(um.Type, Equals, urlTypeksyun)
	c.Assert(um.Scheme, Equals, "http")
	c.Assert(um.NetLoc, Equals, "docs.github.com:8080")

	c.Assert(um.getURL("bucket", "object", "params").String(), Equals, "http://bucket.docs.github.com:8080/object?params")
	c.Assert(um.getURL("bucket", "object", "").String(), Equals, "http://bucket.docs.github.com:8080/object")
	c.Assert(um.getURL("", "object", "").String(), Equals, "http://docs.github.com:8080/")
	c.Assert(conn.getResource("bucket", "object", "subres"), Equals, "/bucket/object?subres")
	c.Assert(conn.getResource("bucket", "object", ""), Equals, "/bucket/object")
	c.Assert(conn.getResource("", "object", ""), Equals, "/")

	um.Init("https://docs.github.com:8080", false, true, false)
	c.Assert(um.Type, Equals, urlTypeksyun)
	c.Assert(um.Scheme, Equals, "https")
	c.Assert(um.NetLoc, Equals, "docs.github.com:8080")

	um.Init("127.0.0.1", false, true, false)
	c.Assert(um.Type, Equals, urlTypeIP)
	c.Assert(um.Scheme, Equals, "http")
	c.Assert(um.NetLoc, Equals, "127.0.0.1")

	um.Init("http://127.0.0.1", false, false, false)
	c.Assert(um.Type, Equals, urlTypeIP)
	c.Assert(um.Scheme, Equals, "http")
	c.Assert(um.NetLoc, Equals, "127.0.0.1")
	c.Assert(um.getURL("bucket", "object", "params").String(), Equals, "http://127.0.0.1/bucket/object?params")
	c.Assert(um.getURL("", "object", "params").String(), Equals, "http://127.0.0.1/?params")

	um.Init("https://127.0.0.1:8080", false, false, false)
	c.Assert(um.Type, Equals, urlTypeIP)
	c.Assert(um.Scheme, Equals, "https")
	c.Assert(um.NetLoc, Equals, "127.0.0.1:8080")

	um.Init("http://[2401:b180::dc]", false, false, false)
	c.Assert(um.Type, Equals, urlTypeIP)
	c.Assert(um.Scheme, Equals, "http")
	c.Assert(um.NetLoc, Equals, "[2401:b180::dc]")

	um.Init("https://[2401:b180::dc]:8080", false, false, false)
	c.Assert(um.Type, Equals, urlTypeIP)
	c.Assert(um.Scheme, Equals, "https")
	c.Assert(um.NetLoc, Equals, "[2401:b180::dc]:8080")
}

func (s *Ks3ConnSuite) TestAuth(c *C) {
	endpoint := "https://github.com/"
	cfg := getDefaultKs3Config()
	cfg.AuthVersion = AuthV1
	um := UrlMaker{}
	um.Init(endpoint, false, false, false)
	conn := Conn{cfg, &um, nil}
	uri := um.getURL("bucket", "object", "")
	req := &http.Request{
		Method:     "PUT",
		URL:        uri,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       uri.Host,
	}

	req.Header.Set("Content-Type", "text/html")
	req.Header.Set("Date", "Thu, 17 Nov 2005 18:49:58 GMT")
	req.Header.Set("Host", endpoint)
	req.Header.Set("X-KS3-Meta-Your", "your")
	req.Header.Set("X-KS3-Meta-Author", "foo@bar.com")
	req.Header.Set("X-KS3-Magic", "abracadabra")
	req.Header.Set("Content-Md5", "ODBGOERFMDMzQTczRUY3NUE3NzA5QzdFNUYzMDQxNEM=")

	conn.signHeader(req, conn.getResource("bucket", "object", ""))
	testLogger.Println("AUTHORIZATION:", req.Header.Get(HTTPHeaderAuthorization))
}

func (s *Ks3ConnSuite) TestConnToolFunc(c *C) {
	err := CheckRespCode(202, []int{})
	c.Assert(err, NotNil)

	err = CheckRespCode(202, []int{404})
	c.Assert(err, NotNil)

	err = CheckRespCode(202, []int{202, 404})
	c.Assert(err, IsNil)

	srvErr, err := serviceErrFromXML([]byte(""), 312, "")
	c.Assert(err, NotNil)
	c.Assert(srvErr.StatusCode, Equals, 0)

	srvErr, err = serviceErrFromXML([]byte("ABC"), 312, "")
	c.Assert(err, NotNil)
	c.Assert(srvErr.StatusCode, Equals, 0)

	srvErr, err = serviceErrFromXML([]byte("<Error></Error>"), 312, "")
	c.Assert(err, IsNil)
	c.Assert(srvErr.StatusCode, Equals, 312)

	unexpect := UnexpectedStatusCodeError{[]int{200}, 202}
	c.Assert(len(unexpect.Error()) > 0, Equals, true)
	c.Assert(unexpect.Got(), Equals, 202)

	fd, err := os.Open("../sample/BingWallpaper-2015-11-07.jpg")
	c.Assert(err, IsNil)
	fd.Close()
	var out ProcessObjectResult
	err = jsonUnmarshal(fd, &out)
	c.Assert(err, NotNil)
}

func (s *Ks3ConnSuite) TestSignRtmpURL(c *C) {
	cfg := getDefaultKs3Config()

	um := UrlMaker{}
	um.Init(endpoint, false, false, false)
	conn := Conn{cfg, &um, nil}

	//Anonymous
	channelName := "test-sign-rtmp-url"
	playlistName := "playlist.m3u8"
	expiration := time.Now().Unix() + 3600
	signedRtmpURL := conn.signRtmpURL(bucketName, channelName, playlistName, expiration)
	playURL := getPublishURL(bucketName, channelName)
	hasPrefix := strings.HasPrefix(signedRtmpURL, playURL)
	c.Assert(hasPrefix, Equals, true)

	//empty playlist name
	playlistName = ""
	signedRtmpURL = conn.signRtmpURL(bucketName, channelName, playlistName, expiration)
	playURL = getPublishURL(bucketName, channelName)
	hasPrefix = strings.HasPrefix(signedRtmpURL, playURL)
	c.Assert(hasPrefix, Equals, true)
}

func (s *Ks3ConnSuite) TestGetRtmpSignedStr(c *C) {
	cfg := getDefaultKs3Config()
	um := UrlMaker{}
	um.Init(endpoint, false, false, false)
	conn := Conn{cfg, &um, nil}

	akIf := conn.config.GetCredentials()

	//Anonymous
	channelName := "test-get-rtmp-signed-str"
	playlistName := "playlist.m3u8"
	expiration := time.Now().Unix() + 3600
	params := map[string]interface{}{}
	signedStr := conn.getRtmpSignedStr(bucketName, channelName, playlistName, expiration, akIf.GetAccessKeySecret(), params)
	c.Assert(signedStr, Equals, "")
}
