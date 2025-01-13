// logger/logger.go
package logger

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
	FATAL
)

type Logger struct {
	level  Level
	logger *log.Logger
	mu     sync.Mutex
}

var (
	defaultLogger *Logger
	once          sync.Once
)

func getLogger() *Logger {
	once.Do(func() {
		defaultLogger = &Logger{
			level: DEBUG,
			// 移除 log.Lshortfile，我们自己处理文件信息
			logger: log.New(os.Stdout, "", log.LstdFlags),
		}
	})
	return defaultLogger
}

func SetLevel(level Level) {
	l := getLogger()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// 获取调用者的文件和行号
func getCaller(skip int) string {
	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		return "???"
	}
	return fmt.Sprintf("%s:%d", filepath.Base(file), line)
}

func output(calldepth int, level Level, format string, v ...interface{}) {
	l := getLogger()
	l.mu.Lock()
	defer l.mu.Unlock()

	if level < l.level {
		return
	}

	msg := fmt.Sprintf(format, v...)
	levelStr := ""
	switch level {
	case DEBUG:
		levelStr = "DEBUG"
	case INFO:
		levelStr = "INFO"
	case WARN:
		levelStr = "WARN"
	case ERROR:
		levelStr = "ERROR"
	case FATAL:
		levelStr = "FATAL"
	}

	// 格式: [级别] 文件:行号 消息
	caller := getCaller(calldepth)
	// 格式: [级别] 文件名:行号 消息
	logMsg := fmt.Sprintf("[%s] %s %s", levelStr, caller, msg)
	l.logger.Output(2, logMsg)

	if level == FATAL {
		os.Exit(1)
	}
}

// 包级别的日志函数
func Debug(format string, v ...interface{}) {
	output(3, DEBUG, format, v...)
}

func Info(format string, v ...interface{}) {
	output(3, INFO, format, v...)
}

func Warn(format string, v ...interface{}) {
	output(3, WARN, format, v...)
}

func Error(format string, v ...interface{}) {
	output(3, ERROR, format, v...)
}

func Fatal(format string, v ...interface{}) {
	output(3, FATAL, format, v...)
}
