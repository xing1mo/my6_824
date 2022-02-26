#/usr/local/go/bin/go build -race -buildmode=plugin  -gcflags="all=-N -l" ../mrapps/wc.go
/usr/local/go/bin/go build -race  -gcflags="all=-N -l" -buildmode=plugin ../mrapps/wc.go
rm -f mr-out-*