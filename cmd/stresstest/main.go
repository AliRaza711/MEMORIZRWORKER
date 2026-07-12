package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}

	dbURL := os.Getenv("DATABASE_URL")
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer pool.Close()

	var validUserID string
	err = pool.QueryRow(ctx, `SELECT id FROM "User" LIMIT 1`).Scan(&validUserID)
	if err != nil {
		// Insert a dummy user if the local DB is completely empty
		validUserID = generateUUID()
		_, err = pool.Exec(ctx, `INSERT INTO "User" (id, email, timezone, updated_at) VALUES ($1, 'test@example.com', 'Asia/Karachi', NOW())`, validUserID)
		if err != nil {
			log.Fatalf("Failed to create dummy user: %v", err)
		}
	}

	absoluteTargetTime := time.Now().Add(-1 * time.Minute).UTC()
	totalReminders := 10000
	batchSize := 1000 

	log.Printf("Starting 10K Local DB Benchmark Setup...")

	for i := 0; i < totalReminders; i += batchSize {
		batch := &pgx.Batch{}
		for j := 0; j < batchSize; j++ {
			batch.Queue(
				`INSERT INTO "Reminder" (id, user_id, title, description, target_datetime, status, retry_count, created_at, updated_at) 
				 VALUES ($1, $2, $3, $4, $5, 'Pending', 0, NOW(), NOW())`,
				generateUUID(), validUserID, fmt.Sprintf("Benchmark %d", i+j), "Benchmark Test", absoluteTargetTime,
			)
		}
		br := pool.SendBatch(ctx, batch)
		_, _ = br.Exec()
		br.Close()
		log.Printf("Inserted records %d to %d...", i, i+batchSize-1)
	}

	log.Printf("✅ Inserted %d reminders to LOCAL DB. Run the worker now!", totalReminders)
}
