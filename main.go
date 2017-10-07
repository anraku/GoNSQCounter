package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	nsq "github.com/bitly/go-nsq"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var fatalErr error

// 発生したエラーをグローバル変数に保持しておき、
// main関数終了後にOSに対して終了コードを指定する
func fatal(e error) {
	fmt.Println(e)
	flag.PrintDefaults()
	fatalErr = e
}

const updateDuration = 1 * time.Second

func main() {
	// deferの実行順はLIFO(後入れ先出し)
	defer func() {
		if fatalErr != nil {
			os.Exit(1)
		}
	}()

	// --- データベース接続 ---
	log.Println("データベースに接続します...")
	db, err := mgo.Dial("localhost")
	if err != nil {
		fatal(err)
		return
	}
	defer func() {
		log.Println("データベース接続を閉じます...")
		db.Close()
	}()
	pollData := db.DB("ballots").C("polls")

	// --- NSQ接続 ---
	log.Println("NSQに接続します...")
	q, err := nsq.NewConsumer("votes", "counter", nsq.NewConfig())
	if err != nil {
		fatal(err)
		return
	}

	// countsの操作をロックするためのインスタンス
	var countsLock sync.Mutex
	// 集計結果を保持するマップ
	var counts map[string]int

	// この関数はvotes上でメッセージを受け取る度に呼ばれる
	q.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		// 他のオブジェクトがロック中だった場合は、この処理でブロックされる
		countsLock.Lock()
		defer countsLock.Unlock()
		if counts == nil {
			counts = make(map[string]int)
		}
		vote := string(m.Body)
		counts[vote]++
		// エラーが起きなかったことを表すnil1
		return nil
	}))

	// nsqlookupdインスタンスに接続
	if err := q.ConnectToNSQLookupd("localhost:4161"); err != nil {
		fatal(err)
		return
	}
	log.Println("NSQ上での投票を待機します...")
	var updater *time.Timer
	updater = time.AfterFunc(updateDuration, func() {
		countsLock.Lock()
		defer countsLock.Unlock()
		// mapに値が追加されているかチェック
		if len(counts) == 0 {
			log.Println("新しい投票はありません。データベースの更新をスキップします。")
		} else {
			// マップに新しい集計結果があるので、DBを更新する
			log.Println("データベースを更新します...")
			log.Println(counts)
			ok := true
			for option, count := range counts {
				sel := bson.M{"options": bson.M{"$in": []string{option}}}
				up := bson.M{"$inc": bson.M{"results." + option: count}}
				if _, err := pollData.UpdateAll(sel, up); err != nil {
					log.Println("更新に失敗しました:", err)
					ok = false
					continue
				}
				counts[option] = 0
			}
			if ok {
				log.Println("データベースの更新が完了しました")
				counts = nil // 得票数をリセットします
			}
		}
		updater.Reset(updateDuration)
	})

	// ctrl + cに対応する
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	for {
		select {
		// シグナルを受信した時
		case <-termChan:
			updater.Stop()
			q.Stop()
		case <-q.StopChan:
			// 完了
			return
		}
	}
}
