# telegraf-processor-aws-imds

Telegraf processor which can tag metrics with data from the AWS IMDS service

### Installation

* Clone the repo

```
git clone git@github.com:tuscanylabs/telegraf-processor-aws-imds.git
```
* Build the "aws-imds" binary

```
$ go build -o aws-imds cmd/main.go
```
* You should be able to call this from telegraf now using execd
```
[[processors.execd]]
  command = ["/path/to/aws-imds", "-config /path/to/config"]
  signal = "none"
```
This self-contained plugin is based on the documentations of [Execd Go Shim](https://github.com/influxdata/telegraf/blob/effe112473a6bd8991ef8c12e293353c92f1d538/plugins/common/shim/README.md)

### Unit Testing

The plugin includes unit tests which can be run with the following

```
go test ./...
```

### Functional Testing

Functional tests can be done with the following process

First, create a new AWS VM instance

Next, ensure that the binary is built for the architecture of the VM you created. For example, on an x86-64 arch,
you can specifically build the binary like so.

```
GOOS=linux GOARCH=amd64 go build -o aws-imds cmd/main.go
```

Next, copy the binary to the instance. You can do this via SCP or any other method you have available. For instance,

```console
scp -i ~/aws-vm-key.cer aws-imds ec2-user@SOME-IP-ADDRESS:.
```

Next, create a sample config in the VM. You can use the config provided [in this repository][1].

```azure
[[processors.aws_imds]]
	imds_tags = ["region"]
```

Next, start the binary with a sample config

```
./aws-imds --config telegraf.conf
2023/03/03 17:12:10 D! Initializing AWS IMDS Processor
```

Now, paste a line of input to the console using the [InfluxDB line protocol format][2].

```azure
weather,foo=bar temperature=82 1465839830100400200
```

You should expect to see the output changed to include a `region` tag. For example,

```
weather,foo=bar,location=eastus temperature=82 1465839830100400200
```

[1]: ./plugins/processors/aws/imds/sample.conf
[2]: https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/