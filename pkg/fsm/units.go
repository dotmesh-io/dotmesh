package fsm

import "fmt"

func formatBytes(i int64) string {
	var f float64 = float64(i)
	switch {
	case i < 0:
		return "-" + formatBytes(-i)
	case i >= 1024*1024*1024*1024:
		return fmt.Sprintf("%.1fTiB", f/(1024.0*1024.0*1024.0*1024.0))
	case i >= 1024*1024*1024:
		return fmt.Sprintf("%.1fGiB", f/(1024.0*1024.0*1024.0))
	case i >= 1024*1024:
		return fmt.Sprintf("%.1fMiB", f/(1024.0*1024.0))
	case i >= 1024:
		return fmt.Sprintf("%.1fKiB", f/1024.0)
	default:
		return fmt.Sprintf("%dB", i)
	}
}
