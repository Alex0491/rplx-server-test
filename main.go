package main

import (
	"net"
	"os"
	"time"

	"fmt"

	"github.com/negasus/rplx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	logger, _ := zap.NewDevelopment()

	rplxInstance1ListenAddr := "127.0.0.1:7501"
	rplxInstance2ListenAddr := "127.0.0.1:7502"

	rplxInstance1 := rplx.New(rplx.WithNodeID("rplx 1"), rplx.WithLogger(logger), rplx.WithGCInterval(time.Second*time.Duration(30)), rplx.WithNodeMaxBufferSize(0))
	if err := rplxInstance1.AddRemoteNode(rplxInstance2ListenAddr, time.Duration(200)*time.Second, grpc.WithInsecure()); err != nil {
		logger.Error("error add remote node to rplx", zap.String("addr", rplxInstance2ListenAddr), zap.Error(err))
		os.Exit(1)
	}
	ln1, err := net.Listen("tcp4", rplxInstance1ListenAddr)
	if err != nil {
		logger.Error("error listen", zap.Error(err))
		os.Exit(1)
	}
	go func() {
		if err := rplxInstance1.StartReplicationServer(ln1); err != nil {
			logger.Error("error start grpc server", zap.Any("err", err))
			os.Exit(1)
		}
	}()

	rplxInstance2 := rplx.New(rplx.WithNodeID("rplx 2"), rplx.WithLogger(logger), rplx.WithGCInterval(time.Second*time.Duration(30)), rplx.WithNodeMaxBufferSize(0))
	if err := rplxInstance2.AddRemoteNode(rplxInstance1ListenAddr, time.Duration(200)*time.Second, grpc.WithInsecure()); err != nil {
		logger.Error("error add remote node to rplx", zap.String("addr", rplxInstance1ListenAddr), zap.Error(err))
		os.Exit(1)
	}
	ln2, err := net.Listen("tcp4", rplxInstance2ListenAddr)
	if err != nil {
		logger.Error("error listen", zap.Error(err))
		os.Exit(1)
	}
	go func() {
		if err := rplxInstance2.StartReplicationServer(ln2); err != nil {
			logger.Error("error start grpc server", zap.Any("err", err))
			os.Exit(1)
		}
	}()

	time.Sleep(6 * time.Second)

	logger.Info("upsert value to instance 1")
	rplxInstance1.Upsert("test-var", 500)

	time.Sleep(1000 * time.Microsecond)

	logger.Info("get value from instance 2")
	val, err := rplxInstance2.Get("test-var")

	if err != nil {
		panic(err)
	}

	if val != 500 {
		panic(fmt.Errorf("%d != 500", val))
	}
}
