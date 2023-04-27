package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Merchant struct {
	RetailerID  int64     `gorm:"column:retailer_id;" json:"retailer_id"`
	BranchCount *int32    `gorm:"branch_count" json:"branch_count"`
	ExpireAt    time.Time `gorm:"column:expire_at;" json:"expire_at"`
}

const (
	defaultMaxOpenConns    = 100
	defaultMaxIdleConns    = 5
	defaultMaxConnLifeTime = time.Hour
	defaultMaxConnIdleTime = 30 * time.Minute
)

var (
	filePath   = "retail.csv"
	tableName  = "merchant"
	columnName = "branch_count"
)

func configConnection(db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}

	sqlDB.SetMaxOpenConns(defaultMaxOpenConns)
	sqlDB.SetMaxIdleConns(defaultMaxIdleConns)
	sqlDB.SetConnMaxLifetime(defaultMaxConnLifeTime)
	sqlDB.SetConnMaxIdleTime(defaultMaxConnIdleTime)

	return nil
}

func newDB() (*gorm.DB, error) {
	logrus.Debug("Coming Create Storage")

	db, err := gorm.Open(mysql.Open(viper.GetString("db.dsn")), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	if err = configConnection(db); err != nil {
		return nil, err
	}

	return db, nil
}

func updateBranchCount(db *gorm.DB, retailerID int64, branchCount *int32) error {
	merchant := new(Merchant)

	cond := clause.Eq{Column: "retailer_id", Value: retailerID}

	if err := db.Table(tableName).Clauses(cond).Take(merchant).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			logrus.Infof("Retailer ID %d not found", retailerID)
			return nil
		}
		return err
	}

	if merchant.BranchCount == nil {
		logrus.Infof("Update branch count to %d", *branchCount)
		if err := db.Table(tableName).Clauses(cond).Update(columnName, *branchCount).Error; err != nil {
			return err
		}
	} else {
		logrus.Infof("Branch count already set to %d", *merchant.BranchCount)
	}

	return nil
}

func init() {
	var pathConfig string
	flag.StringVar(&pathConfig, "config", "config.yaml", "path to config file")
	flag.Parse()

	err := readConfig(pathConfig)
	if err != nil {
		logrus.Fatalf("Failed to read config from file %s: %+v\n", viper.ConfigFileUsed(), err)
	}
}

func readConfig(pathConfig string) error {
	viper.SetConfigFile(pathConfig)
	viper.AutomaticEnv()

	return viper.ReadInConfig()
}

func fileLoader(pipe chan<- Merchant, wg *sync.WaitGroup) error {
	defer wg.Done()

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer func() {
		err = file.Close()
		if err != nil {
			logrus.Errorf("Cannot close file with error: %+v", err)
		}
	}()

	var merchant Merchant
	reader := csv.NewReader(file)
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		retailerID, err := strconv.ParseInt(line[0], 10, 64)
		if err != nil {
			logrus.Errorf("Error parser Retailer ID: %+v", err)
			return err
		}

		branchCountInt64, err := strconv.ParseInt(line[1], 10, 64)
		if err != nil {
			logrus.Errorf("Error parser Branch Count: %+v", err)
			return err
		}
		branchCount := int32(branchCountInt64)

		merchant.BranchCount = &branchCount
		merchant.RetailerID = retailerID

		pipe <- merchant
	}

	return nil
}

func handleMerchant() {
	db, err := newDB()
	if err != nil {
		logrus.Error(err)
	}

	var wg sync.WaitGroup

	pipe := make(chan Merchant)
	done := make(chan bool)

	go func() {
		for {
			merchant, more := <-pipe
			if more {
				logrus.Infof("Handle update branch count merchant with retailer id %d", merchant.RetailerID)
				if err := updateBranchCount(db, merchant.RetailerID, merchant.BranchCount); err != nil {
					logrus.Error(err)
				}
			} else {
				logrus.Info("Done")
				done <- true
				return
			}
		}
	}()

	wg.Add(1)

	go func() {
		err := fileLoader(pipe, &wg)
		if err != nil {
			logrus.Errorf("Error when load file: %+v", err)
		}
	}()

	go func() {
		wg.Wait()
		close(pipe)
	}()
	<-done
}

func main() {
	handleMerchant()
}
