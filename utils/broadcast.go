package utils

func Broadcast(infoQueue map[string]chan string, message string) {
	for k := range infoQueue {
		infoQueue[k] <- message
	}
}
