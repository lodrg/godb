package logger

import (
	"testing"
)

func TestLoggerAlignment(t *testing.T) {
	// 测试列对齐功能
	SetColumnWidths(8, 25)
	SetColorEnabled(false) // 关闭颜色以便测试对齐

	// 测试不同长度的消息
	Debug("short")
	Info("medium length message")
	Warn("very long message that should still align properly")
	Error("another message")
	Fatal("final test message")
}

func TestLoggerColors(t *testing.T) {
	// 测试颜色功能
	SetColorEnabled(true)
	SetColumnWidths(8, 25)

	Debug("debug message with color")
	Info("info message with color")
	Warn("warn message with color")
	Error("error message with color")
	Fatal("fatal message with color")
}

func TestLoggerLevels(t *testing.T) {
	// 测试日志级别过滤
	SetLevel(INFO)
	SetColorEnabled(false)

	Debug("this should not appear")
	Info("this should appear")
	Warn("this should appear")
	Error("this should appear")

	// 重置为DEBUG级别
	SetLevel(DEBUG)
}
