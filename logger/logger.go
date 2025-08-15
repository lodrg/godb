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

// ANSI color codes
const (
	colorReset   = "\x1b[0m"
	colorGray    = "\x1b[90m"
	colorGreen   = "\x1b[32m"
	colorYellow  = "\x1b[33m"
	colorRed     = "\x1b[31m"
	colorMagenta = "\x1b[35m"
)

type Logger struct {
	level       Level
	logger      *log.Logger
	mu          sync.Mutex
	levelWidth  int
	callerWidth int
	useColor    bool
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
			logger:      log.New(os.Stdout, "", log.LstdFlags),
			levelWidth:  5,  // 固定列宽: [LEVEL]
			callerWidth: 25, // 固定列宽: 文件:行号
			useColor:    true,
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

// 开关: 是否启用彩色输出
func SetColorEnabled(enabled bool) {
	l := getLogger()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.useColor = enabled
}

// 设置对齐列宽: levelWidth 为级别列宽, callerWidth 为调用点(文件:行号)列宽
// 当实际文本长度超过设置的列宽时，将右对齐保留末尾，并在需要时裁剪左侧
func SetColumnWidths(levelWidth, callerWidth int) {
	l := getLogger()
	l.mu.Lock()
	defer l.mu.Unlock()
	if levelWidth > 0 {
		l.levelWidth = levelWidth
	}
	if callerWidth > 0 {
		l.callerWidth = callerWidth
	}
}

// 获取调用者的文件和行号
func getCaller(skip int) string {
	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		return "???"
	}
	// 使用相对路径用于IDE跳转
	return fmt.Sprintf("%s:%d", filepath.Base(file), line)
}

func fitWidthLeftAlign(s string, width int) string {
	if width <= 0 {
		return s
	}
	runes := []rune(s)
	if len(runes) > width {
		return string(runes[:width])
	}
	return fmt.Sprintf("%-*s", width, s)
}

func colorize(level Level, text string, useColor bool) string {
	if !useColor {
		return text
	}
	switch level {
	case DEBUG:
		return colorGray + text + colorReset
	case INFO:
		return colorGreen + text + colorReset
	case WARN:
		return colorYellow + text + colorReset
	case ERROR:
		return colorRed + text + colorReset
	case FATAL:
		return colorMagenta + text + colorReset
	default:
		return text
	}
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

	// 列: [LEVEL] 调用点 消息
	caller := getCaller(calldepth)
	paddedLevel := fmt.Sprintf("%-*s", l.levelWidth, levelStr)
	coloredLevel := colorize(level, paddedLevel, l.useColor)
	callerCol := fitWidthLeftAlign(caller, l.callerWidth)

	logMsg := fmt.Sprintf("[%s] %s %s", coloredLevel, callerCol, msg)
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
