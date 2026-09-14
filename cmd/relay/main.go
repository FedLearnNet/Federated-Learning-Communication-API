package main

import (
	"crypto/tls"
	"fc_controller/pkg/relayserver/bridge"
	"fc_controller/pkg/relayserver/http"
	"fc_controller/pkg/relayserver/tcp"
	"fc_controller/pkg/shared/logger"
	"fc_controller/pkg/shared/util"
	"fmt"
	"github.com/jessevdk/go-flags"
	"os"
)

const RELAY string = "RELAY"
const CONFIG_FILE string = "config.yml"

type Config struct {
	Relay struct {
		Mode           string `yaml:"mode" long:"mode" env:"RELAY_MODE"`
		HTTPPort       int    `yaml:"httpPort" long:"http-port" env:"RELAY_HTTP_PORT"`
		TCPPort        int    `yaml:"tcpPort" long:"tcp-port" env:"RELAY_TCP_PORT"`
		LogLevelStdOut string `yaml:"logLevelStdOut" long:"logLevelStdOut" env:"LOG_LEVEL_STDOUT"`
		LogLevelFile   string `yaml:"logLevelFile" long:"logLevelFile" env:"LOG_LEVEL_FILE"`
		// Either "off", "self-signed", or "on". If "on", the relay will use the provided TLS certificate and key. If "self-signed", the relay will generate a self-signed certificate for TLS.
		TLSMode       string  `yaml:"tlsMode" long:"tls-mode" env:"RELAY_TLS_MODE"`
		TLSMinVersion *uint16 `yaml:"tlsMinVersion" long:"tls-min-version" env:"RELAY_TLS_MIN_VERSION"`
		TLSMaxVersion *uint16 `yaml:"tlsMaxVersion" long:"tls-max-version" env:"RELAY_TLS_MAX_VERSION"`
		// Comma seperated list of domains for which the TLS certificate is valid.
		// Only for self-signed mode, otherwise uses whatever the given certs say
		TLSDomains        *string `yaml:"domain" long:"domain" env:"RELAY_DOMAIN"`
		TLSPublicCertPath *string `yaml:"publicCertPath" long:"public-cert-path" env:"RELAY_PUBLIC_CERT_PATH"`
		TLSPrivateKeyPath *string `yaml:"privateKeyPath" long:"private-key-path" env:"RELAY_PRIVATE_KEY_PATH"`
	} `yaml:"relay"`
}

func uint16Ptr(v uint16) *uint16 {
	return &v
}

func NewConfig() *Config {
	cfg := &Config{}
	// Mode is either "prod" or "dev"
	// E.g. using self signed certs is only possible in dev
	cfg.Relay.Mode = "prod"
	cfg.Relay.HTTPPort = 9140
	cfg.Relay.TCPPort = 9141
	cfg.Relay.LogLevelStdOut = "info"
	cfg.Relay.LogLevelFile = "debug"
	cfg.Relay.TLSMode = "on" // either "off", "self-signed", or "on"
	cfg.Relay.TLSPublicCertPath = nil
	cfg.Relay.TLSPrivateKeyPath = nil
	cfg.Relay.TLSDomains = nil
	cfg.Relay.TLSMaxVersion = nil
	cfg.Relay.TLSMinVersion = uint16Ptr(uint16(tls.VersionTLS13))
	return cfg
}

func main() {
	fmt.Println(" _____      _             ____                          ")
	fmt.Println("|  __ \\    | |           / ___|                         ")
	fmt.Println("| |__) |___| | __ _ _   _\\___ \\ ___ _ ____   _____ _ __")
	fmt.Println("|  _  // _ \\ |/ _` | | | |___) / _ \\ '__\\ \\ / / _ \\ '__|")
	fmt.Println("| | \\ \\  __/ | (_| | |_| |____/  __/ |   \\ V /  __/ |   ")
	fmt.Println("|_|  \\_\\___|_|\\__,_|\\__, |     \\___|_|    \\_/ \\___|_|   ")
	fmt.Println("                      __/ |                              ")
	fmt.Println("                     |___/                               ")

	var cliConfig Config
	_, err := flags.ParseArgs(&cliConfig, os.Args)
	if err != nil {
		panic("CLI args error: " + err.Error())
	}

	// Load configuration with priority: CLI -> ENV -> YAML -> defaults
	confInterface, err := util.LoadConfig(CONFIG_FILE, NewConfig(), &cliConfig)
	if err != nil {
		panic("Config loading error: " + err.Error())
	}

	util.PrintConfig(confInterface)

	conf := confInterface.(*Config)
	logPath := logger.DEFAULTLOGDIR
	_ = os.MkdirAll(logPath, os.ModePerm)
	logger.Setup(logPath, conf.Relay.LogLevelStdOut, conf.Relay.LogLevelFile)
	logger.Info(RELAY, "", "Logger initialized. Logs directory: %s", logPath)

	logger.Info(RELAY, "", "Initializing relay server...")
	store := bridge.NewFlRunStore()

	errs := make(chan error, 2)
	httpServer := http.NewHTTPServer(store)
	var tcpServer *tcp.RelayServiceTCP = nil
	go func() {
		logger.Info(RELAY, "", "Configuring TLS for the TCP federation relay...")
		tcpServer, err = tcp.NewTCPServer(store, conf.Relay.Mode, conf.Relay.TLSMode, conf.Relay.TLSPublicCertPath, conf.Relay.TLSPrivateKeyPath, conf.Relay.TLSDomains, conf.Relay.TLSMinVersion, conf.Relay.TLSMaxVersion)
		if err != nil {
			errs <- err
			return
		}
		errs <- tcpServer.StartServer(conf.Relay.TCPPort)
	}()

	go func() {
		logger.Info(RELAY, "", "Starting HTTP API on port %d", conf.Relay.HTTPPort)
		errs <- httpServer.StartServer(conf.Relay.HTTPPort)
	}()

	for serverErr := range errs {
		if serverErr != nil {
			logger.Fatal(RELAY, "", "Server error: %v", serverErr)
		}
	}
}
