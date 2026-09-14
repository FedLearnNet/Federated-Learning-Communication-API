package main

import (
	"fc_controller/pkg/controller"
	"fc_controller/pkg/shared/logger"
	"fc_controller/pkg/shared/util"
	"fmt"
	"github.com/jessevdk/go-flags"
	"os"
	"os/signal"
	"syscall"
)

const MAIN string = "MAIN"
const CONFIG_FILE string = "config.yml"

type Config struct {
	Manager struct {
		FLRunManagerPort     int    `yaml:"flrunmanagerport" long:"flrunmanagerport" env:"FL_RUN_MANAGER_PORT"`
		AppCommV2Port        int    `yaml:"appCommV2Port" long:"appcommv2port" env:"APPCOMMV2_PORT"`
		LogLevelStdOut       string `yaml:"logLevelStdOut" long:"logLevelStdOut" env:"LOG_LEVEL_STDOUT"`
		LogLevelFile         string `yaml:"logLevelFile" long:"logLevelFile" env:"LOG_LEVEL_FILE"`
		LearningApiWsAddress string `yaml:"learningApiWsAddress" long:"learning-api-ws" env:"WORKFLOW_LEARNING_API_WS_ADDRESS"`
	} `yaml:"manager"`
	Relay struct {
		Mode       string `yaml:"mode" long:"mode" env:"RELAY_MODE"`
		TLSMode    string `yaml:"tlsMode" long:"tls-mode" env:"RELAY_TLS_MODE"`
		AddressTCP string `yaml:"addressTCP" long:"address-tcp" env:"RELAY_ADDRESS_TCP"`
	} `yaml:"relay"`
}

// NewConfig returns a Config with sensible defaults
func NewConfig() *Config {
	cfg := &Config{}
	cfg.Manager.FLRunManagerPort = 8002
	cfg.Manager.AppCommV2Port = 8003
	cfg.Manager.LogLevelStdOut = "info"
	cfg.Manager.LogLevelFile = "debug"
	cfg.Manager.LearningApiWsAddress = "ws://localhost:8080/ws"
	cfg.Relay.Mode = "prod"
	cfg.Relay.TLSMode = "on"
	cfg.Relay.AddressTCP = "localhost:9150"
	return cfg
}

func main() {
	fmt.Println("  _____            _             _ _           ")
	fmt.Println(" / ____|          | |           | | |          ")
	fmt.Println("| |     ___  _ __ | |_ _ __ ___ | | | ___ _ __ ")
	fmt.Println("| |    / _ \\| '_ \\| __| '__/ _ \\| | |/ _ \\ '__|")
	fmt.Println("| |___| (_) | | | | |_| | | (_) | | |  __/ |   ")
	fmt.Println(" \\_____\\___/|_| |_|\\__|_|  \\___/|_|_|\\___|_|   ")
	fmt.Println("                                                 ")

	// Parse CLI flags into config
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

	// Print loaded config
	util.PrintConfig(confInterface)

	conf := confInterface.(*Config)
	// SET LOGGER
	logPath := logger.DEFAULTLOGDIR
	_ = os.MkdirAll(logPath, os.ModePerm)
	logger.Setup(logPath, conf.Manager.LogLevelStdOut, conf.Manager.LogLevelFile)
	logger.Info(MAIN, "", "Logger initialized. Logs directory: %s", logPath)

	dockerized := os.Getenv("FC_DOCKERIZED") == "1"
	if dockerized {
		logger.Info(MAIN, "", "Mode: Dockerized (isolated)")
	} else {
		logger.Info(MAIN, "", "Mode: Direct execution")
	}

	// START CONTROLLER
	logger.Info(MAIN, "", "Starting Controller...")
	run_manager := controller.NewFLRunManagerServiceHTTP(
		conf.Manager.FLRunManagerPort,
		conf.Manager.AppCommV2Port,
		conf.Manager.LearningApiWsAddress,
		conf.Relay.AddressTCP,
		conf.Relay.Mode,
		conf.Relay.TLSMode,
	)
	// Create a channel for errors
	errs := make(chan error, 1)

	// Start the controller in a background goroutine
	go func() {
		// We send the result of StartServer into the channel
		errs <- run_manager.StartServer()
	}()

	// Create a channel to listen for OS signals (Interrupt/Terminate)
	importSignals := make(chan os.Signal, 1)
	signal.Notify(importSignals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errs:
		if err != nil {
			logger.Fatal(MAIN, "", "Controller crashed: %v", err)
		}
	case sig := <-importSignals:
		logger.Info(MAIN, "", "Received signal %v, shutting down gracefully...", sig)

		// 5. Call your shutdown logic
		run_manager.Shutdown()
	}

	logger.Info(MAIN, "", "Application exited.")

}
