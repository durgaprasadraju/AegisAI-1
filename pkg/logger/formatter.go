package logger

// Formatter formats log entries
type Formatter interface {
	Format(level string, msg string, fields []Field) []byte
}

// JSONFormatter formats logs as JSON
type JSONFormatter struct{}

// Format formats a log entry as JSON
func (f *JSONFormatter) Format(level string, msg string, fields []Field) []byte {
	// TODO: Implement JSON formatting
	return []byte{}
}
