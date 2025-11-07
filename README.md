# Unofficial Postgres Exporter for OTEL

![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/wabb-in/postgresexporter)
![GitHub Repo stars](https://img.shields.io/github/stars/wabb-in/postgresexporter)

*Sponsorship and merging the exporter into the OpenTelemetry Collector Contrib project are planned. The discussion is happening in this issue -> https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/35451*
<hr/>

This repository contains an unofficial implementation for an <a href="https://opentelemetry.io/">OpenTelemetry</a> Exporter to export otel logs, traces and metrics to PostgreSQL or other databases built on top of PostgreSQL.

## Usage

To try this exporter you can build a custom OTEL Collector.

You can follow the official instructions [here](https://opentelemetry.io/docs/collector/custom-collector/), but a quick setup is provided below.
Feel free to experiment with the configs in the `otellocalcol-example` directory.

1. Copy .env.example to .env.
```sh
cp .env.example .env
```

2. Download the OpenTelemetry Collector Builder (ocb) and build a collector.
```sh
make download/ocb
make build
```

*NOTE*: Builder version should be same as the otel collector version i.e. v0.139.0 in this case. Check your `OCB_DOWNLOAD_PATH` env.

3. Run the collector to start collecting measurements

```sh
./otellocalcol-example/otellocalcol-example --config="./otellocalcol-example/otel-collector-config.yml"
```

## Supported Functionalities
- Export OTEL Logs to Postgres (development)
- Export OTEL Traces to Postgres (development)
- Export OTEL Metrics to Postgres (development)

## Contributing
Please check out the issues section of the repository to contribute to this project, all contributions are highly welcomed.
