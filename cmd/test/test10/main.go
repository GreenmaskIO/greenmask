package main

import (
	"context"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"time"
)

type test struct {
}

func main() {
	ch := make(chan *test, 100)

	eg := &errgroup.Group{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eg.Go(func() error {
		defer func() {
			close(ch)
			log.Debug().Msg("closed channel")
		}()
		for i := 0; i < 10; i++ {
			log.Debug().Msg("sent to chan")
			ch <- &test{}
		}
		return nil
	})

	eg.Go(func() error {
		for {
			select {
			case res, ok := <-ch:
				if !ok {
					log.Debug().Any("res1", res).Msg("closed")
					return nil
				}
				log.Debug().Any("res1", res).Msg("received")
			case <-ctx.Done():

			}
			time.Sleep(100 * time.Millisecond)
		}
	})

	eg.Go(func() error {
		for {
			select {
			case res, ok := <-ch:
				if !ok {
					log.Debug().Any("res2", res).Msg("closed")
					return nil
				}
				log.Debug().Any("res2", res).Msg("received")
			case <-ctx.Done():

			}
			time.Sleep(100 * time.Millisecond)
		}
	})

	err := eg.Wait()
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
}
