package logger

import (
	"bufio"
	"fmt"
	"github.com/sirupsen/logrus"
	logger "log"
	"os"
	"path"
	"sync"
	"time"
)

const DEFAULTLOGDIR = "./logs"

var (
	log                          *logrus.Logger
	LogDir                       string
	logLevelStdOut, logLevelFile logrus.Level
	logFile                      *os.File
	lock                         sync.Mutex
)

func init() {
	log = logrus.New()
}

func Setup(logDir string, loglevelstdout string, loglevelfile string) {

	switch loglevelstdout {
	case "debug":
		logLevelStdOut = logrus.DebugLevel
	case "info":
		logLevelStdOut = logrus.InfoLevel
	case "warn":
		logLevelStdOut = logrus.WarnLevel
	case "error":
		logLevelStdOut = logrus.ErrorLevel
	case "fatal":
		logLevelStdOut = logrus.FatalLevel
	default:
		logLevelStdOut = logrus.DebugLevel
	}

	// Parse file log level
	switch loglevelfile {
	case "debug":
		logLevelFile = logrus.DebugLevel
	case "info":
		logLevelFile = logrus.InfoLevel
	case "warn":
		logLevelFile = logrus.WarnLevel
	case "error":
		logLevelFile = logrus.ErrorLevel
	case "fatal":
		logLevelFile = logrus.FatalLevel
	default:
		logLevelFile = logrus.DebugLevel
	}

	t := time.Now()
	var fileName = path.Join(logDir, "log-"+t.Format("2006-01-02_15-04-05")+".log")
	createLogFile(fileName)
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		logger.Fatalf("error opening file: %v", err)
	}

	logFile = f
	log.SetReportCaller(false)
}

func Logs(fromLine int) ([]string, error) {
	var logLines []string

	lock.Lock()
	defer lock.Unlock()
	_, err := logFile.Seek(0, 0)

	if err != nil {
		return logLines, err
	}
	scanner := bufio.NewScanner(logFile)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		logLines = append(logLines, scanner.Text())
	}

	if fromLine > 0 && fromLine <= len(logLines) {
		return logLines[fromLine:], nil
	}

	return logLines, nil
}

func createLogFile(filePath string) {
	// if directory already exists, do nothing
	_, err := os.Stat(filePath)
	if err == nil {
		return
	}

	dir := path.Dir(filePath)
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		logger.Fatalf("error creating log directory: %v", err)
	}
	os.Create(filePath)
}

func changeOutputSettingsToFile() {
	log.Level = logLevelFile
	log.SetFormatter(&logrus.JSONFormatter{})
	log.SetOutput(logFile)
}

func changeOutputSettingsToStdOut() {
	log.Level = logLevelStdOut
	log.SetOutput(os.Stdout)
	log.SetFormatter(&logrus.TextFormatter{
		DisableColors:   false,
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	})
}

func logToStdOut(level logrus.Level, component string, instance string, message string, v ...interface{}) {
	lock.Lock()
	defer lock.Unlock()
	changeOutputSettingsToStdOut()
	log.Log(level, fmt.Sprintf("[%s] %s %s", component, instance, fmt.Sprintf(message, v...)))
}

func logToFile(level logrus.Level, component string, instance string, message string, v ...interface{}) {
	lock.Lock()
	defer lock.Unlock()
	changeOutputSettingsToFile()
	fields := logrus.Fields{
		"component": component,
		"instance":  instance,
	}
	log.WithFields(fields).Logf(level, message, v...)
}

func Debug(component string, instance string, message string, v ...interface{}) {
	instanceStr := instanceToString(instance)
	if logLevelStdOut >= logrus.DebugLevel {
		logToStdOut(logrus.DebugLevel, component, instanceStr, message, v...)
	}
	if logLevelFile >= logrus.DebugLevel {
		logToFile(logrus.DebugLevel, component, instanceStr, message, v...)
	}
}

func instanceToString(instance interface{}) string {
	instanceStr := ""
	switch inst := instance.(type) {
	case string:
		if inst != "" {
			instanceStr = fmt.Sprintf("[Instance: %s]", inst)
		}
	case int:
		instanceStr = fmt.Sprintf("[Project: %d]", inst)
	}
	return instanceStr
}

func Info(component string, instance interface{}, message string, v ...interface{}) {
	instanceStr := instanceToString(instance)
	if logLevelStdOut >= logrus.InfoLevel {
		logToStdOut(logrus.InfoLevel, component, instanceStr, message, v...)
	}
	if logLevelFile >= logrus.InfoLevel {
		logToFile(logrus.InfoLevel, component, instanceStr, message, v...)
	}
}

func Warn(component string, instance interface{}, message string, v ...interface{}) {
	instanceStr := instanceToString(instance)
	if logLevelStdOut >= logrus.WarnLevel {
		logToStdOut(logrus.WarnLevel, component, instanceStr, message, v...)
	}
	if logLevelFile >= logrus.WarnLevel {
		logToFile(logrus.WarnLevel, component, instanceStr, message, v...)
	}
}

func Error(component string, instance interface{}, message string, v ...interface{}) {
	instanceStr := instanceToString(instance)
	if logLevelStdOut >= logrus.ErrorLevel {
		logToStdOut(logrus.ErrorLevel, component, instanceStr, message, v...)
	}
	if logLevelFile >= logrus.ErrorLevel {
		logToFile(logrus.ErrorLevel, component, instanceStr, message, v...)
	}
}

func Fatal(component string, instance interface{}, message string, v ...interface{}) {
	instanceStr := instanceToString(instance)
	if logLevelStdOut >= logrus.FatalLevel {
		logToStdOut(logrus.FatalLevel, component, instanceStr, message, v...)
	}
	if logLevelFile >= logrus.FatalLevel {
		logToFile(logrus.FatalLevel, component, instanceStr, message, v...)
	}
}
