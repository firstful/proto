package main

import (
	"embed"
	"flag"
	"log"
	"net/http"

	"gitea.ffilt.us/reeds/proto/proto"
)

//go:embed webapp
var webappFS embed.FS

func main() {
	addr := flag.String("addr", ":8808", "http listen address")
	journal := flag.String("journal", "", "JSONL journal path (crash recovery)")
	flag.Parse()

	webapp, err := webappFS.ReadFile("webapp/index.html")
	if err != nil {
		log.Fatalf("webapp: %v", err)
	}

	b := proto.NewBroker(*journal)
	if err := b.Restore(); err != nil {
		log.Fatalf("journal restore: %v", err)
	}
	srv := proto.NewServer(b)
	log.Printf("proto broker listening on %s (ws /ws, api /api/*, ui /)", *addr)
	log.Fatal(http.ListenAndServe(*addr, srv.Handler(webapp)))
}
