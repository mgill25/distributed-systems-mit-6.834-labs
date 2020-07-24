package raft

import (
	"fmt"
	"log"
)

// Debugging
const DebugMode = 3

/*
	Debug = 3
	Info = 2
	Warn = 1
	Error = 0
*/

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugMode > 0 {
		log.Printf(format, a...)
	}
	return
}

func Min(x int, y int) int {
	if x < y {
		return x
	}
	return y
}

var (
	InfoColor  = Green
	DebugColor = White
	WarnColor  = Yellow
	FatalColor = Red
)

var (
	Black   = Color("\033[1;30m%s\033[0m")
	Red     = Color("\033[1;31m%s\033[0m")
	Green   = Color("\033[1;32m%s\033[0m")
	Yellow  = Color("\033[1;33m%s\033[0m")
	Purple  = Color("\033[1;34m%s\033[0m")
	Magenta = Color("\033[1;35m%s\033[0m")
	Teal    = Color("\033[1;36m%s\033[0m")
	White   = Color("\033[1;37m%s\033[0m")
)

func Color(colorString string) func(*Raft, string, string, ...interface{}) string {
	sprint := func(rf *Raft, mode string, format string, args ...interface{}) string {
		args = append([]interface{}{rf.me, rf.currentTerm, rf.state}, args...)
		str := fmt.Sprintf("[%s]", mode)
		str += " [Node: %d | Term: %d | %10.10v] " + format
		formatString := fmt.Sprintf(str, args...)
		return fmt.Sprintf(colorString, formatString)
	}
	return sprint
}

// Logging functions
// Debug: White
func Debug(rf *Raft, format string, a ...interface{}) (n int, err error) {
	if DebugMode >= 3 {
		log.Printf("%s\n", White(rf, "DEBUG", format, a...))
	}
	return
}

// Info: Green
func Info(rf *Raft, format string, a ...interface{}) (n int, err error) {
	if DebugMode >= 2 {
		log.Printf("%s\n", Green(rf, "INFO", format, a...))
	}
	return
}

// Warn: Yellow
func Warn(rf *Raft, format string, a ...interface{}) (n int, err error) {
	if DebugMode >= 1 {
		log.Printf("%s\n", Yellow(rf, "WARN", format, a...))
	}
	return
}

// Error: Red
func Error(rf *Raft, format string, a ...interface{}) (n int, err error) {
	if DebugMode >= 0 {
		log.Printf("%s\n", Red(rf, "ERROR", format, a...))
	}
	return
}
