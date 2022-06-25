package myMongo

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/square/mongo-lock"
)

var LockId = "global_lock"
var MongoLock *lock.Client

func init() {
	mongoUrl := "mongodb://124.220.208.74:27017"
	database := "lotus"
	collection := "lock"

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	m, err := mongo.Connect(ctx, options.Client().
		ApplyURI(mongoUrl).
		SetWriteConcern(writeconcern.New(writeconcern.WMajority())))

	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err = m.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	// Configure the client for the database and collection the lock will go into.
	col := m.Database(database).Collection(collection)

	// Create a MongoDB lock client.
	MongoLock = lock.NewClient(col)

	// Create the required and recommended indexes.
	MongoLock.CreateIndexes(ctx)
}

func main() {
	// Create a Mongo session and set the write mode to "majority".
	mongoUrl := "youMustProvideThis"
	database := "lotus"
	collection := "collectionName"

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	m, err := mongo.Connect(ctx, options.Client().
		ApplyURI(mongoUrl).
		SetWriteConcern(writeconcern.New(writeconcern.WMajority())))

	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err = m.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	// Configure the client for the database and collection the lock will go into.
	col := m.Database(database).Collection(collection)

	// Create a MongoDB lock client.
	c := lock.NewClient(col)

	// Create the required and recommended indexes.
	c.CreateIndexes(ctx)

	// Create an exclusive lock on resource1.
	err = c.XLock(ctx, "resource1", LockId, lock.LockDetails{})
	if err != nil {
		log.Fatal(err)
	}

	// Create a shared lock on resource2.
	err = c.SLock(ctx, "resource2", LockId, lock.LockDetails{}, -1)
	if err != nil {
		log.Fatal(err)
	}

	// Unlock all locks that have our lockId.
	_, err = c.Unlock(ctx, LockId)
	if err != nil {
		log.Fatal(err)
	}
}
