package gomes

import (
	"log"
	"net/http"
	"net/http/httptest"
)

func makeMockServer(handler func(rsp http.ResponseWriter, req *http.Request)) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(handler))
	log.Println("Created server  " + server.URL)
	return server
}
