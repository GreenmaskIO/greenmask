package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

var eg errgroup.Group

func startWorkers(ctx context.Context, c *sync.Cond, wg *sync.WaitGroup, val *int) {
	workerCount := 3

	for i := 0; i < workerCount; i++ {
		go func(workerId int) {
			eg.Go(func() error {
				fmt.Printf("\ttrying to lock %d\n", workerId)
				c.L.Lock()
				fmt.Printf("\tlocked %d\n", workerId)
				fmt.Printf("\tstarting loop %d\n", workerId)
				wg.Done()
				for {
					fmt.Printf("\twaiting %d\n", workerId)
					select {
					case <-ctx.Done():

						fmt.Printf("\tcontext cancelled %d\n", workerId)
						return nil
					default:
					}
					c.Wait()
					fmt.Printf("\twaited %d\n", workerId)
					fmt.Printf("\tsleeping %d\n", workerId)
					time.Sleep(time.Duration(workerId) + 100*time.Millisecond)
					fmt.Printf("\tsleepped %d\n", workerId)
					// получили сигнал
					fmt.Printf("\tval %v processed by worker %v\n", *val, workerId)
					wg.Done()
				}
			})

		}(i)
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	//defer cancel()
	wg := &sync.WaitGroup{}
	c := sync.NewCond(&sync.Mutex{})
	val := -1
	wg.Add(3)
	startWorkers(ctx, c, wg, &val)
	wg.Wait()

	for i := 0; i < 4; i++ {
		wg.Add(3)
		val = i
		fmt.Printf("main: set val to %v\n", val)

		fmt.Printf("main: locking\n")
		c.L.Lock()
		fmt.Printf("main: locked\n")
		time.Sleep(100 * time.Millisecond)

		fmt.Printf("main: broadcasting all\n")
		c.Broadcast()
		fmt.Printf("main: broadcasted\n")
		time.Sleep(100 * time.Millisecond)

		fmt.Printf("main: unlocking\n")
		c.L.Unlock()
		fmt.Printf("main: unlocked\n")
		time.Sleep(100 * time.Millisecond)

		fmt.Printf("main: waiting\n")
		wg.Wait()
		time.Sleep(100 * time.Millisecond)
		fmt.Printf("main: waited\n")
	}
	cancel()
	c.L.Lock()
	c.Broadcast()
	c.L.Unlock()
	wg.Wait()
	eg.Wait()
}
