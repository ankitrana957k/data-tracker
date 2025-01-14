package service

import (
	"fmt"

	"github.com/krogertechnology/data-tracker/datastore/sqlite"
)

func Migrations() error {
	db := sqlite.CreateDB()
	defer db.Close()

	err := db.DeleteExistingData()
	if err != nil {
		return err
	}

	err = db.CreateTable()
	if err != nil {
		return err
	}

	err = db.AddIdx()
	if err != nil {
		return err
	}

	fmt.Println("Migrations completed successfully.")

	return nil
}
