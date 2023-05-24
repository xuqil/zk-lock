package main

import (
	"context"
	"github.com/go-zookeeper/zk"
	zklock "github.com/xuqil/zk-lock"
	"log"
	"os"
	"os/signal"
	"time"
)

var servers = []string{"localhost:2181"}
var notify = make(chan struct{})

func main() {
	conn, _, err := zk.Connect(servers, 5*time.Second)
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()

	for i := 0; i < 100; i++ {
		if i != 0 && i%10 == 0 {
			go run(conn, "key", i)
		} else {
			go run(conn, "key-", i)
		}
	}
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, os.Kill)
	<-quit
	close(notify)

}

func run(conn *zk.Conn, key string, i int) {
	for {
		select {
		case <-notify:
			return
		default:
			lock := zklock.NewLock(conn, key, zk.WorldACL(zk.PermAll))

			if err := lock.Lock(context.Background()); err != nil {
				log.Fatalf("goroutine[%d] key[%s] lock failed: %s", i, key, err)
			}
			log.Printf("goroutine[%d] key[%s] lock", i, key)
			time.Sleep(10 * time.Millisecond)

			if err := lock.Unlock(context.Background()); err != nil {
				log.Fatalf("goroutine[%d] key[%s] unlock failed: %s", i, key, err)
			}
			log.Printf("goroutine[%d] key[%s] unlock", i, key)
		}
	}
}
