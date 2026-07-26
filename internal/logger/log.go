package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

const (
	debugLevel = iota
	infoLevel
	warnLevel
	errorLevel
)

type Logger struct {
	base  *log.Logger
	level int
	file  *os.File
}

func New(level, filePath string) (*Logger, error) {
	parsedLevel, err := parseLevel(level)
	if err != nil {
		return nil, err
	}

	if filePath == "" || filePath == "-" {
		return create(parsedLevel, os.Stdout, nil), nil
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file %q: %w", filePath, err)
	}
	return create(parsedLevel, file, file), nil
}

func create(level int, writer io.Writer, file *os.File) *Logger {
	return &Logger{
		base:  log.New(writer, "", log.Ldate|log.Ltime|log.Lmicroseconds|log.LUTC),
		level: level,
		file:  file,
	}
}

func parseLevel(level string) (int, error) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "", "info":
		return infoLevel, nil
	case "debug":
		return debugLevel, nil
	case "warn":
		return warnLevel, nil
	case "error":
		return errorLevel, nil
	default:
		return 0, fmt.Errorf("unsupported log level %q", level)
	}
}

func (l *Logger) Debugf(format string, args ...any) {
	l.write(debugLevel, "DEBUG", format, args...)
}

func (l *Logger) Infof(format string, args ...any) {
	l.write(infoLevel, "INFO", format, args...)
}

func (l *Logger) Warnf(format string, args ...any) {
	l.write(warnLevel, "WARN", format, args...)
}

func (l *Logger) Errorf(format string, args ...any) {
	l.write(errorLevel, "ERROR", format, args...)
}

func (l *Logger) write(level int, name, format string, args ...any) {
	if level < l.level {
		return
	}
	l.base.Printf(name+" "+format, args...)
}

func (l *Logger) Close() error {
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}
