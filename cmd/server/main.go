package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/convin/webhook-ingest/internal/config"
	"github.com/convin/webhook-ingest/internal/httpapi"
	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/redisclient"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

const shutdownTimeout = 10 * time.Second

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()
	ctx := context.Background()

	st, err := store.New(ctx, cfg.PostgresDSN, cfg.DBMaxConns)
	if err != nil {
		log.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	rdb, err := redisclient.New(ctx, cfg.RedisAddr)
	if err != nil {
		log.Error("connect redis", "err", err)
		os.Exit(1)
	}
	defer func() { _ = rdb.Close() }()

	cache := stats.NewCache()
	if err := hydrateCache(ctx, st, cache); err != nil {
		log.Error("load account stats", "err", err)
		os.Exit(1)
	}

	svc := ingest.New(st, cache, rdb, log)
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: httpapi.NewRouter(svc, log)}

	go func() {
		log.Info("listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server stopped", "err", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Info("shutting down")

	httpCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(httpCtx); err != nil {
		log.Error("shutdown", "err", err)
	}

	// srv.Shutdown only waits for HTTP handlers to return, and Ingest
	// returns before its background recording work is done. Wait for that
	// too, on its own timeout rather than whatever's left of the one above -
	// a slow HTTP drain shouldn't shrink the window background work gets
	// before the deferred store/redis closes below pull the rug out from
	// under it, which is the exact failure this exists to prevent.
	bgCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := svc.Shutdown(bgCtx); err != nil {
		log.Error("background work did not finish before shutdown timeout", "err", err)
	}
}

// hydrateCache seeds the in-memory stats cache from the durable aggregate,
// so GET /accounts/{id}/stats reflects reality immediately after a restart
// instead of reading back to zero until fresh events repopulate it.
func hydrateCache(ctx context.Context, st *store.Store, cache *stats.Cache) error {
	totals, err := st.AllAccountStats(ctx)
	if err != nil {
		return err
	}

	seed := make(map[string]stats.AccountStats, len(totals))
	for accountID, total := range totals {
		seed[accountID] = stats.AccountStats{
			CallCount:        total.CallCount,
			TotalDurationSec: total.TotalDurationSec,
		}
	}
	cache.Seed(seed)
	return nil
}
