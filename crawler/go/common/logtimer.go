package common

import "time"

type LogTimer struct {
	logTicker *time.Ticker
    logDone chan bool
    isRunning bool
} 


func (logTimer *LogTimer) Start(duration time.Duration, callback func()) {

    if logTimer.isRunning == true {
        logTimer.Stop()
    }

	logTimer.logTicker = time.NewTicker(duration)
	logTimer.logDone = make(chan bool)

	go func() {
        for {
            select {
            case <-logTimer.logDone:
                return
            case <-logTimer.logTicker.C:
            	callback()
            }
        }
    }()
}


func (logTimer *LogTimer) Stop() {

    if logTimer.isRunning == true {
        logTimer.logTicker.Stop()
        logTimer.logDone <- true

        logTimer.isRunning = false
    }
}