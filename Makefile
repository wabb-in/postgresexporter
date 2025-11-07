include .env

.PHONY: download/ocb
download/ocb:
	curl --proto '=https' --tlsv1.2 -fL -o ocb ${OCB_DOWNLOAD_PATH}
	chmod +x ocb

.PHONY: build
build:
	CGO_ENABLED=0 ./ocb --config ${OTELCOL_PATH}/builder-config.yaml