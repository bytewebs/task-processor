// main.go
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/joho/godotenv"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
}

// Task represents a task in the database
type Task struct {
	ID        int
	Payload   string
	Status    string
	Result    sql.NullString
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Config holds application configuration
type Config struct {
	DatabaseURL   string
	WorkerCount   int
	FetchInterval time.Duration
	FetchLimit    int
}

// Application represents the task processor application
type Application struct {
	config Config
	db     *sql.DB
	logger *log.Logger
}

// loadConfig loads configuration from environment variables
func loadConfig() Config {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/taskprocessor?sslmode=disable"
	}

	workerCountStr := os.Getenv("WORKER_COUNT")
	workerCount := 5
	if workerCountStr != "" {
		if count, err := strconv.Atoi(workerCountStr); err == nil {
			workerCount = count
		}
	}

	fetchIntervalStr := os.Getenv("FETCH_INTERVAL")
	fetchInterval := 5 * time.Second
	if fetchIntervalStr != "" {
		if interval, err := strconv.Atoi(fetchIntervalStr); err == nil {
			fetchInterval = time.Duration(interval) * time.Second
		}
	}

	fetchLimitStr := os.Getenv("FETCH_LIMIT")
	fetchLimit := 10
	if fetchLimitStr != "" {
		if limit, err := strconv.Atoi(fetchLimitStr); err == nil {
			fetchLimit = limit
		}
	}

	return Config{
		DatabaseURL:   dbURL,
		WorkerCount:   workerCount,
		FetchInterval: fetchInterval,
		FetchLimit:    fetchLimit,
	}
}

func connectDB(databaseURL string) (*sql.DB, error) {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

func (app *Application) runMigrations() error {
	app.logger.Println("Running database migration...")

	migrationSQL, err := os.ReadFile("migrations/001_create_tasks_table.sql")
	if err != nil {
		return fmt.Errorf("failed to read migration file: %w", err)
	}

	_, err = app.db.Exec(string(migrationSQL))
	if err != nil {
		return fmt.Errorf("failed to run migration: %w", err)
	}

	app.logger.Println("Migration completed successfully")
	return nil
}

func (app *Application) fetchTasks(ctx context.Context, limit int) ([]Task, error) {
	tx, err := app.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(
		ctx,
		`SELECT id, payload 
		 FROM tasks 
		 WHERE status = 'pending' 
		 ORDER BY created_at 
		 LIMIT $1 
		 FOR UPDATE SKIP LOCKED`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending tasks: %w", err)
	}

	var tasks []Task
	var ids []int
	for rows.Next() {
		var task Task
		if err := rows.Scan(&task.ID, &task.Payload); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan task row: %w", err)
		}
		tasks = append(tasks, task)
		ids = append(ids, task.ID)
	}
	rows.Close()

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %w", err)
	}

	if len(tasks) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
		return tasks, nil
	}

	for _, id := range ids {
		_, err = tx.ExecContext(
			ctx,
			`UPDATE tasks SET status = 'in_progress', updated_at = NOW() WHERE id = $1`,
			id,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to update task %d to in_progress: %w", id, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	app.logger.Printf("Fetched %d tasks", len(tasks))
	return tasks, nil
}

func (app *Application) processTask(ctx context.Context, workerID int, task Task) error {
	app.logger.Printf("[Worker %d] Processing task ID: %d with payload: %s", workerID, task.ID, task.Payload)

	processingTime := rand.Intn(3000) + 1000
	timer := time.NewTimer(time.Duration(processingTime) * time.Millisecond)

	select {
	case <-timer.C:
	case <-ctx.Done():
		timer.Stop()
		return fmt.Errorf("task processing cancelled: %w", ctx.Err())
	}

	result := reverseString(task.Payload)

	failed := rand.Intn(10) == 0

	status := "done"
	var resultString sql.NullString
	resultString.Valid = true
	resultString.String = result

	if failed {
		status = "error"
		resultString.String = "Error processing task"
	}

	_, err := app.db.ExecContext(
		ctx,
		`UPDATE tasks SET status = $1, result = $2, updated_at = NOW() WHERE id = $3`,
		status, resultString, task.ID,
	)
	if err != nil {
		return fmt.Errorf("failed to update task status: %w", err)
	}

	app.logger.Printf("[Worker %d] Task ID: %d processed with status: %s", workerID, task.ID, status)
	return nil
}

func reverseString(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func (app *Application) worker(ctx context.Context, wg *sync.WaitGroup, workerID int, taskCh <-chan Task) {
	defer wg.Done()

	for {
		select {
		case task, ok := <-taskCh:
			if !ok {
				return
			}
			if err := app.processTask(ctx, workerID, task); err != nil {
				app.logger.Printf("[Worker %d] Error processing task %d: %v", workerID, task.ID, err)
			}
		case <-ctx.Done():
			app.logger.Printf("[Worker %d] Received shutdown signal", workerID)
			return
		}
	}
}

func (app *Application) runTaskProcessor() error {
	app.logger.Printf("Starting task processor with %d workers, fetching every %v",
		app.config.WorkerCount, app.config.FetchInterval)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	taskCh := make(chan Task)

	var wg sync.WaitGroup
	wg.Add(app.config.WorkerCount)
	for i := 0; i < app.config.WorkerCount; i++ {
		workerID := i + 1
		app.logger.Printf("Starting worker %d", workerID)
		go app.worker(ctx, &wg, workerID, taskCh)
	}

	fetcherDone := make(chan struct{})
	go func() {
		defer close(fetcherDone)
		ticker := time.NewTicker(app.config.FetchInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				tasks, err := app.fetchTasks(ctx, app.config.FetchLimit)
				if err != nil {
					app.logger.Printf("Error fetching tasks: %v", err)
					continue
				}
				for _, task := range tasks {
					select {
					case taskCh <- task:
					case <-ctx.Done():
						return
					}
				}
			case <-ctx.Done():
				app.logger.Println("Task fetcher received shutdown signal")
				return
			}
		}
	}()

	<-sigCh
	app.logger.Println("Shutdown signal received, stopping task processor gracefully...")
	cancel()

	<-fetcherDone
	close(taskCh)
	wg.Wait()
	app.logger.Println("All workers have completed, shutdown successful")
	return nil
}

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)
	config := loadConfig()

	migrateCmd := flag.NewFlagSet("migrate", flag.ExitOnError)
	serveCmd := flag.NewFlagSet("serve", flag.ExitOnError)

	if len(os.Args) < 2 {
		logger.Println("Expected 'migrate' or 'serve' subcommands")
		os.Exit(1)
	}

	logger.Printf("Connecting to database: %s", config.DatabaseURL)
	db, err := connectDB(config.DatabaseURL)
	if err != nil {
		logger.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	app := &Application{
		config: config,
		db:     db,
		logger: logger,
	}

	switch os.Args[1] {
	case "migrate":
		migrateCmd.Parse(os.Args[2:])
		if err := app.runMigrations(); err != nil {
			logger.Fatalf("Migration failed: %v", err)
		}
	case "serve":
		serveCmd.Parse(os.Args[2:])
		if err := app.runTaskProcessor(); err != nil {
			logger.Fatalf("Task processor failed: %v", err)
		}
	default:
		logger.Println("Expected 'migrate' or 'serve' subcommands")
		os.Exit(1)
	}
}
