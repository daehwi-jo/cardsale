package cls

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net/http"
	"time"
)

var WebHome string

const (
	TIMEOUT_CONNECTION = 4
	TIMEOUT_RESPONSE   = 10
)

func HttpRequest(types string, method string, svrIp string, svrPort string, uri string, close bool) (*http.Response, error) {
	return HttpRequestDetail(types, method, svrIp, svrPort, uri, nil, nil, "", close)
}

func HttpsJson(method, svrIp, svrPort, uri string, body []byte) (*http.Response, error) {
	if body != nil{
		return HttpRequestDetail("HTTPS", method, svrIp, svrPort, uri, body, nil, "application/json", true)
	}
	return HttpRequestDetail("HTTPS", method, svrIp, svrPort, uri, nil, nil, "application/json", true)
}

func HttpGet(svrIp, svrPort, uri string) (*http.Response, error) {
	return HttpRequestDetail("HTTP", "GET", svrIp, svrPort, uri, nil, nil, "", true)
}

func HttpRequestDetail(types string, method string, svrIp string, svrPort string, uri string, body []byte, reqHeader map[string]string, contentType string, close bool) (*http.Response, error) {

	var address string

	// https 추가시에 http.Transport에 TLS 관련 로직 추가
	if types == "HTTP" {
		address = fmt.Sprintf("http://%s:%s/%s", svrIp, svrPort, uri)
	} else if types == "HTTPS" {
		address = fmt.Sprintf("https://%s:%s/%s", svrIp, svrPort, uri)
	}

	// client object
	client := &http.Client{
		Timeout: time.Second * TIMEOUT_RESPONSE,
		//Transport: // https info
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	Lprintf(4, "[INFO] %s send address (%s)\n", types, address)

	req, err := http.NewRequest(method, address, bytes.NewBuffer(body))
	if err != nil {
		Lprintf(1, "[ERROR] http NewRequest error(%s) \n", err.Error())
		return nil, err
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	if close {
		req.Header.Set("Connection", "close")
	}

	// set request header
	if reqHeader != nil {
		for name, value := range reqHeader {
			req.Header.Set(name, value)
		}
	}

	// client transport
	resp, err := client.Do(req)
	if err != nil {
		Lprintf(1, "[ERROR] http client do error(%s) \n", err.Error())
		/*
			// host not found : no such host
			// con close : connection refused
			// client timeout : Timeout exceeded
			if strings.Contains(err.Error(), "connection refused") { // next target
				resp, err = client.Do(req)
				if err != nil { // next target
					continue
				}
			}
		*/
	}

	return resp, err
}

func HttpNotifyCh(types string, method string, svrIp string, svrPort string, uri string, body []byte, reqHeader map[string]string, contentType string, close bool, ch chan string) {

	var address string

	// https 추가시에 http.Transport에 TLS 관련 로직 추가
	if types == "HTTP" {
		address = fmt.Sprintf("http://%s:%s/%s", svrIp, svrPort, uri)
	} else if types == "HTTPS" {
		address = fmt.Sprintf("https://%s:%s/%s", svrIp, svrPort, uri)
	}

	// client object
	client := &http.Client{
		Timeout: time.Second * TIMEOUT_RESPONSE,
		//Transport: // https info
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	Lprintf(4, "[INFO] send address to (%s)\n", address)

	req, err := http.NewRequest(method, address, bytes.NewBuffer(body))
	if err != nil {
		Lprintf(1, "[ERROR] http NewRequest error(%s) \n", err.Error())
		ch <- svrIp
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	if close {
		req.Header.Set("Connection", "close")
	}

	// set request header
	if reqHeader != nil {
		for name, value := range reqHeader {
			req.Header.Set(name, value)
		}
	}

	// client transport
	resp, err := client.Do(req)
	if err != nil {
		Lprintf(1, "[ERROR] http client do error(%s) \n", err.Error())
		/*
			// host not found : no such host
			// con close : connection refused
			// client timeout : Timeout exceeded
			if strings.Contains(err.Error(), "connection refused") { // next target
				resp, err = client.Do(req)
				if err != nil { // next target
					continue
				}
			}
		*/
		ch <- svrIp
	} else {
		defer resp.Body.Close()
		ch <- resp.Status
	}
}
