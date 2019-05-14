package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/pkg/errors"

	cli "github.com/jawher/mow.cli"
)

var (
	app = cli.App("kafkacli", "Kafkacli")

	bootstrapServers = app.Strings(cli.StringsOpt{
		Name:   "b broker brokers",
		Value:  []string{"localhost:9092"},
		EnvVar: "KAFKACLI_BROKER",
		Desc:   "brokers",
	})
	useSSL = app.Bool(cli.BoolOpt{
		Name: "s secure",
		Desc: "use SSL",
	})
	sslCAFile = app.String(cli.StringOpt{
		Name:   "ssl-cafile",
		EnvVar: "KAFKACLI_SSL_CA_FILE",
		Desc:   "filename of CA file to use in certificate verification",
	})
	sslCertFile = app.String(cli.StringOpt{
		Name:   "ssl-certfile",
		EnvVar: "KAFKACLI_SSL_CERT_FILE",
		Desc:   "filename of file in PEM format containing the client certificate",
	})
	sslKeyFile = app.String(cli.StringOpt{
		Name:   "ssl-keyfile",
		EnvVar: "KAFKACLI_SSL_KEY_FILE",
		Desc:   "filename containing the client private key",
	})
)

func main() {
	app.Spec = "[-b...] [-s] [--ssl-cafile] [--ssl-certfile] [--ssl-keyfile]"

	app.Command("consume", "consume and display messages from 1 or more topics", consumeCmd)
	app.Command("produce", "produce a message into 1 or more topics", produceCmd)

	die(app.Run(os.Args))
}

func config(useSSL bool, sslCAFile string, sslCertFile string, sslKeyFile string) *cluster.Config {
	config := cluster.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.Group.Return.Notifications = true

	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	if useSSL || sslCAFile != "" || sslCertFile != "" || sslKeyFile != "" {
		config.Net.TLS.Enable = true
	}

	config.Net.TLS.Config = &tls.Config{}

	if sslCertFile != "" || sslKeyFile != "" {
		if sslCertFile == "" && sslKeyFile == "" {
			die(errors.New("You need to specify both ssl-certfile and ssl-keyfile"))
		}

		cer, err := tls.LoadX509KeyPair(sslCertFile, sslKeyFile)
		die(err)

		config.Net.TLS.Config.Certificates = []tls.Certificate{cer}
	}

	if sslCAFile != "" {
		caCert, err := ioutil.ReadFile(sslCAFile)
		die(err)

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		config.Net.TLS.Config.RootCAs = caCertPool
	}
	return config
}

func die(err error) {
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %v\n", err)
		cli.Exit(1)
	}
}

func splitFlatten(cs []string) []string {
	var res []string
	for _, c := range cs {
		parts := strings.Split(c, ",")
		for _, p := range parts {
			res = append(res, strings.TrimSpace(p))
		}
	}
	return res
}
