package main

import (
	"OttoDB/server"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	figure "github.com/common-nighthawk/go-figure"
)

func main() {

	figure.NewFigure("OttoDB", "larry3d", true).Print()

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		done <- true
	}()

	go server.RunServer()

	fmt.Println("OttoDB Running at port 8080")
	<-done
	figure.NewFigure("Tah - Tah", "larry3d", true).Print()
}
