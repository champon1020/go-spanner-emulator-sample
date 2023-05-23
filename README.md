# Spanner Emulatorを用いたGoのテスト実装

「Spanner Emulatorを用いたGoのテスト実装」のサンプルコード。

## Run Spanner Emulator

```shell
docker run -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator
```

## Run Test

``` shell
go test -v ./...
```
