package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"s3-dedup/internal/command"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("127.0.0.1:6060", nil))
	}()
	command.Execute()
}
