package sqlite

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Store struct {
	*sql.DB
}

func CreateDB() Store {
	db, err := sql.Open("sqlite3", "./records.db")
	if err != nil {
		log.Fatal(err)
	}

	return Store{db}
}

func (s *Store) DeleteExistingData() error {
	query := `DROP TABLE IF EXISTS records;`

	_, err := s.DB.Exec(query)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) CreateTable() error {
	query := `CREATE TABLE IF NOT EXISTS records (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		topic TEXT NOT NULL,
		partition TEXT NOT NULL,
		offset TEXT NOT NULL,
        message JSONB NOT NULL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
	);`

	_, err := s.DB.Exec(query)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) AddIdx() error {
	query := `CREATE INDEX IF NOT EXISTS record_idx ON records(topic, partition, offset);`

	_, err := s.DB.Exec(query)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) Create(topic, partition, message, offset string) error {
	query := `INSERT INTO records (topic, partition, offset, message, timestamp) VALUES (?,?,?,?,?)`

	_, err := s.DB.Exec(query, topic, partition, offset, message, time.Now())
	if err != nil {
		return err
	}

	return nil
}
