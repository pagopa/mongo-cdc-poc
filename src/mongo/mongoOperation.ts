/* eslint-disable no-console */
import * as TE from "fp-ts/TaskEither";
import {
  Binary,
  ChangeStream,
  ChangeStreamDocument,
  Collection,
  Db,
  Document,
  InsertOneResult,
  MongoClient,
  OptionalUnlessRequiredId,
} from "mongodb";
import { ResumeToken } from "../model/resumeToken";

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
    async () => {
      const collections = await db
        .listCollections({ name: collectionName })
        .toArray();
      if (collections.length === 0) {
        return db.createCollection<T>(collectionName);
      }
      return db.collection(collectionName);
    },
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
  collection: Collection<T>,
  resumeToken: string
): TE.TaskEither<Error, ChangeStream<T, ChangeStreamDocument<T>>> =>
  TE.tryCatch(
    async () => {
      // eslint-disable-next-line functional/no-let
      let params:
        | { fullDocument: string }
        | { fullDocument: string; resumeAfter: { _data: unknown } } = {
        fullDocument: "updateLookup",
      };

      if (resumeToken !== undefined) {
        params = {
          ...params,
          resumeAfter: {
            _data: new Binary(Buffer.from(resumeToken, "base64")),
          },
        };
      }
      return collection.watch(
        [
          {
            $match: {
              operationType: { $in: ["insert", "update", "replace"] },
            },
          },
          { $project: { _id: 1, fullDocument: 1, ns: 1, documentKey: 1 } },
        ],
        params
      );
    },
    (reason) =>
      new Error(
        `Impossible to watch the ${collection.namespace} collection: " ${reason}`
      )
  );

export const findLastToken = <T>(
  collection: Collection<T>
): TE.TaskEither<Error, ResumeToken> =>
  TE.tryCatch(
    async () => {
      const myDocument = await collection
        .find<ResumeToken>({})
        .sort({ _id: -1 })
        .limit(1)
        .tryNext();
      if (myDocument) {
        return myDocument;
      }
      return null;
    },
    (reason) =>
      new Error(
        `Unable to get the last inserted document from collection: " ${reason}`
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
