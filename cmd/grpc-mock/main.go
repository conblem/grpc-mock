package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	log "github.com/golang/glog"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	api "github.com/conblem/grpc-mock/pkg/server/api"
	"github.com/conblem/grpc-mock/pkg/server/mock"
	"github.com/conblem/grpc-mock/pkg/stub"
	mockpb "github.com/conblem/grpc-mock/proto/mock"
)

var (
	importPaths = StringsValue{Delimiter: ","}
	protoFiles  = StringsValue{Delimiter: ","}
	stubFiles   = StringsValue{Delimiter: ","}
	stubDir     string

	mockAddr string
	apiAddr  string
)

func init() {
	flag.Var(&importPaths, "import-path", "The path to a directory from which proto sources can be imported, for use with -proto flags. Multiple import paths should be separated by comma")
	flag.Var(&protoFiles, "proto", "The names of the proto source files.")
	flag.Var(&stubFiles, "stub-files", "The names of stub files.")
	flag.StringVar(&stubDir, "stub-dir", "", "The dir of stub files.")
	flag.StringVar(&mockAddr, "mock-addr", ":22222", "The address mock server listens")
	flag.StringVar(&apiAddr, "api-addr", ":22220", "The address api server listens")
}

func main() {
	flag.Parse()

	fds, err := parseProtos(importPaths.Elements, protoFiles.Elements)
	if err != nil {
		log.Fatalf("parse proto files failed: %v", err)
	}
	if len(fds) == 0 {
		log.Infoln("no parsed file descriptors")
		return
	}

	err = registerFileDescriptors(fds)
	if err != nil {
		log.Fatalf("register file descriptors failed: %v", err)
	}

	stubMgr := stub.NewManager()
	if stubDir != "" {
		err = stubMgr.LoadStubsFromFile(stubDir)
		if err != nil {
			log.Fatalf("load stubs from file failed: %v", err)
		}
	}
	go startAPIServer(apiAddr, api.NewServer(stubMgr))

	mockServer := mock.NewServer(mockAddr, stubMgr)
	mockServer.RegisterServices(fds)
	err = mockServer.Start()
	if err != nil {
		log.Fatalf("start mock server failed: %v", err)
	}
	fmt.Println("grpc server is listening on ", mockAddr)
	defer mockServer.Stop()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGPIPE, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT)
	for {
		select {
		case sig := <-signals:
			switch sig {
			case syscall.SIGPIPE:
			case syscall.SIGINT:
				fallthrough
			default:
				return
			}
		}
	}
}

func parseProtos(importPaths, protoFiles []string) ([]*desc.FileDescriptor, error) {
	parser := protoparse.Parser{
		ImportPaths: importPaths,
	}
	return parser.ParseFiles(protoFiles...)
}

func registerFileDescriptors(fds []*desc.FileDescriptor) (err error) {
	var registry *protoregistry.Files
	fdset := desc.ToFileDescriptorSet(fds...)
	registry, err = protodesc.NewFiles(fdset)
	if err != nil {
		return err
	}
	registry.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		if ofd, _ := protoregistry.GlobalFiles.FindFileByPath(fd.Path()); ofd != nil {
			return true
		}

		if regErr := protoregistry.GlobalFiles.RegisterFile(fd); regErr != nil {
			log.Infof("register proto '%s' failed: %v", fd.Path(), regErr)
			return false
		}
		log.Infoln("register proto", fd.Path())
		return true
	})
	return
}

func startAPIServer(addr string, svr mockpb.MockServer) {
	lsn, err := net.Listen("tcp4", addr)
	if err != nil {
		log.Fatalf("api server listen failed: %v", err)
	}

	mux := runtime.NewServeMux()
	mockpb.RegisterMockHandlerServer(context.Background(), mux, svr)

	log.Infof("api server starts on %v", lsn.Addr().String())
	fmt.Println("api server is listening on ", lsn.Addr().String())
	err = http.Serve(lsn, mux)
	if err != nil {
		log.Fatalf("api server serve failed: %v", err)
	}
}

