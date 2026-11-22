// Package grpctest wires a minimal in-process gRPC server and client for the
// interceptor tests: one unary method whose handler the caller supplies. It uses
// the well-known emptypb message so no protoc or generated code is required.
package grpctest

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

// FullMethod is the single unary method's full name.
const FullMethod = "/grpctest.Service/Call"

// ServiceServer is the interface the test service implements. grpc.ServiceDesc
// requires HandlerType to be an interface, because RegisterService reflect-checks
// that the registered implementation implements it.
type ServiceServer interface {
	Call(context.Context) error
}

// Service is the hand-written implementation of the single method.
type Service struct {
	// Handle is called once per request; return a status error or nil for OK.
	Handle func(ctx context.Context) error
}

// Call implements ServiceServer, delegating to the injected Handle.
func (s *Service) Call(ctx context.Context) error {
	if s.Handle == nil {
		return nil
	}
	return s.Handle(ctx)
}

var desc = grpc.ServiceDesc{
	ServiceName: "grpctest.Service",
	HandlerType: (*ServiceServer)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Call",
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			if err := dec(new(emptypb.Empty)); err != nil {
				return nil, err
			}
			handle := func(ctx context.Context) (any, error) {
				return new(emptypb.Empty), srv.(ServiceServer).Call(ctx)
			}
			if interceptor == nil {
				return handle(ctx)
			}
			info := &grpc.UnaryServerInfo{Server: srv, FullMethod: FullMethod}
			return interceptor(ctx, new(emptypb.Empty), info, func(ctx context.Context, _ any) (any, error) {
				return handle(ctx)
			})
		},
	}},
}

// Start registers srv on an in-process server and returns a connected client.
func Start(t *testing.T, srv *Service, serverOpts []grpc.ServerOption, clientOpts []grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer(serverOpts...)
	s.RegisterService(&desc, srv)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	opts := append([]grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, clientOpts...)
	conn, err := grpc.NewClient("passthrough:///bufnet", opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// Call invokes the single unary method and returns its status error, if any.
func Call(ctx context.Context, conn *grpc.ClientConn) error {
	return conn.Invoke(ctx, FullMethod, new(emptypb.Empty), new(emptypb.Empty))
}
