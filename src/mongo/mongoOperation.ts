import * as TE from "fp-ts/TaskEither";
import {
  ChangeStream,
  ChangeStreamDocument,
  Collection,
  Db,
  Document,
  InsertOneResult,
  MongoClient,
  OptionalUnlessRequiredId,
} from "mongodb";

export const mongoConnect = (uri: string): TE.TaskEither<Error, MongoClient> =>
  TE.tryCatch(
    () => MongoClient.connect(uri),
    (reason) =>
      new Error(`Impossible to connect to MongoDB: " ${String(reason)}`)
  );

export const getMongoDb = (
  client: MongoClient,
  dbName: string
): TE.TaskEither<Error, Db> =>
  TE.tryCatch(
    async () => client.db(dbName),
    (reason) =>
      new Error(`Impossible to connect to Get the ${dbName} db: " ${reason}`)
  );

export const getMongoCollection = <T = Document>(
  db: Db,
  collectionName: string
): TE.TaskEither<Error, Collection<T>> =>
  TE.tryCatch(
    async () => db.collection<T>(collectionName),
    (reason) =>
      new Error(
        `Impossible to connect to Get the ${collectionName} collection: " ${reason}`
      )
  );

export const disconnectMongo = (
  client: MongoClient
): TE.TaskEither<Error, void> =>
  TE.tryCatch(
    async () => client.close(),
    (reason) => new Error(`Impossible to close the mongo client: " ${reason}`)
  );

export const watchMongoCollection = <T = Document>(
  collection: Collection<T>
): TE.TaskEither<Error, ChangeStream<T, ChangeStreamDocument<T>>> =>
  TE.tryCatch(
    async () => collection.watch(),
    (reason) =>
      new Error(
        `Impossible to watch the ${collection.namespace} collection: " ${reason}`
      )
  );

export const setMongoListenerOnEventChange = <T = Document>(
  changeStream: ChangeStream<T, ChangeStreamDocument<T>>,
  listener: (change: ChangeStreamDocument<T>) => void
): TE.TaskEither<Error, ChangeStream<T, ChangeStreamDocument<T>>> =>
  TE.tryCatch(
    async () => changeStream.on("change", listener),
    (reason) => new Error(`Impossible to set the listener: " ${reason}`)
  );

export const mongoInsertOne = <T>(
  collection: Collection<T>,
  doc: OptionalUnlessRequiredId<T>
): TE.TaskEither<Error, InsertOneResult<T>> =>
  TE.tryCatch(
    () => collection.insertOne(doc),
    (reason) => new Error(String(reason))
  );
