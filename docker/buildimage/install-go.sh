#! /bin/bash -xe

GOLANG_VERSION=${1:?}
GOLANG_DOWNLOAD_SHA256=${2:?}
GOPATH="/go" 

export PATH="$GOPATH/bin:/usr/local/go/bin:$PATH"

mkdir -p "$GOPATH/src" "$GOPATH/bin" "$GOPATH/scripts"
chmod -R 777 "$GOPATH"
curl -so /tmp/golang.tar.gz "https://storage.googleapis.com/golang/go$GOLANG_VERSION.linux-amd64.tar.gz"
echo "$GOLANG_DOWNLOAD_SHA256 */tmp/golang.tar.gz" | sha256sum -c -
tar -xzf /tmp/golang.tar.gz -C /usr/local
rm -f /tmp/golang.tar.gz
