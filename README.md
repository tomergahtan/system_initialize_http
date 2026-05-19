docker buildx build \
 --platform linux/amd64,linux/arm64 \
 -t ghcr.io/tomergahtan/stock_reset:1.0.13 \
 -t ghcr.io/tomergahtan/stock_reset:latest \
 --push .
