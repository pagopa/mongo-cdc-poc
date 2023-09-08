/* eslint-disable no-console */
import {
  AzureEventhubSasFromString,
  KafkaProducerCompact,
  fromSas,
  sendMessages,
} from "@pagopa/fp-ts-kafkajs/dist/lib/KafkaProducerCompact";
import { defaultLog, useWinston, withConsole } from "@pagopa/winston-ts";
import dotenv from "dotenv";
import * as E from "fp-ts/Either";
import * as TE from "fp-ts/lib/TaskEither";
import { pipe } from "fp-ts/lib/function";
import { ChangeStreamDocument, MongoClient } from "mongodb";
import { CosmosDBConfig, EH_CONFIG, MONGO_CONFIG } from "./config/config";
import { transform } from "./mapper/students";
import { Student } from "./model/student";
import {
  disconnectMongo,
  getMongoCollection,
  getMongoDb,
  mongoConnect,
  mongoInsertOne,
  setMongoListenerOnEventChange,
  watchMongoCollection,
} from "./mongo/mongoOperation";

dotenv.config();
useWinston(withConsole());

const databaseName = "mongo-cdc-poc-mongodb";
const collectionName = "students";

const getCosmosConnectionURI = (): TE.TaskEither<Error, string> =>
  pipe(
    CosmosDBConfig.decode(MONGO_CONFIG),
    E.map((config) => config.CONNECTION_URI),
    TE.fromEither,
    TE.mapLeft((errors) =>
      pipe(
        defaultLog.taskEither.error(
          `Error during decoding Cosmos ConnectionURI - ${errors}`
        ),
        () => new Error(`Error during decoding Cosmos ConnectionURI`)
      )
    )
  );

const getEventHubProducer = (): TE.TaskEither<
  Error,
  KafkaProducerCompact<Student>
> =>
  pipe(
    AzureEventhubSasFromString.decode(EH_CONFIG.CONNECTION_STRING),
    E.map((sas) => fromSas<Student>(sas)),
    TE.fromEither,
    TE.mapLeft((errors) =>
      pipe(
        defaultLog.taskEither.error(
          `Error during decoding Cosmos ConnectionURI - ${errors}`
        ),
        () => new Error(`Error during decoding Event Hub SAS`)
      )
    )
  );

const simulateAsyncPause: TE.TaskEither<Error, void> = TE.tryCatch(
  () =>
    new Promise<void>((resolve) => {
      setTimeout(() => resolve(), 1000);
    }),
  (error) =>
    pipe(
      defaultLog.taskEither.error(`Error during pause simulation - ${error}`),
      () => new Error("An error occurred")
    )
);

const waitForExit = (client: MongoClient): TE.TaskEither<Error, void> =>
  TE.tryCatch(
    async () => {
      process.stdin.resume();
      process.on("SIGINT", () => {
        pipe(disconnectMongo(client), process.exit(0));
      });
    },
    (reason) =>
      pipe(
        defaultLog.taskEither.error(
          `Failed to set exit handler: ${String(reason)}`
        ),
        () => new Error(`Failed to set exit handler`)
      )
  );

const exitFromProcess = (): TE.TaskEither<Error, void> =>
  pipe(defaultLog.taskEither.error(`Application failed`), process.exit(1));

const sendMessageEventHub =
  (messagingClient: KafkaProducerCompact<Student>) =>
  <T = Document>(change: ChangeStreamDocument<T>): void =>
    void pipe(change, transform, (students) =>
      sendMessages(messagingClient)(students)()
    );

const main = () =>
  pipe(
    TE.Do,
    getCosmosConnectionURI,
    defaultLog.taskEither.info("Connecting to mongo..."),
    TE.bind("client", (connectionUri) => mongoConnect(connectionUri)),
    TE.bind("db", ({ client }) => getMongoDb(client, databaseName)),
    TE.bind("collection", ({ db }) => getMongoCollection(db, collectionName)),
    defaultLog.taskEither.info(
      `Connected to DB ${databaseName} - Working on collection ${collectionName}`
    ),
    defaultLog.taskEither.info(
      "Trying to connect to the event hub instance..."
    ),
    TE.bind("messagingClient", () => getEventHubProducer()),
    defaultLog.taskEither.info("Connectied to event hub"),
    defaultLog.taskEither.info(
      `Trying to watch the collection ${collectionName}`
    ),
    TE.chainFirst(({ collection, messagingClient }) =>
      pipe(
        watchMongoCollection(collection),
        TE.chain((watcher) =>
          setMongoListenerOnEventChange(
            watcher,
            sendMessageEventHub(messagingClient)
          )
        )
      )
    ),
    defaultLog.taskEither.info(`Watching the collection ${collectionName}`),
    TE.chainFirst((_) => simulateAsyncPause),
    defaultLog.taskEither.info(`Inserting one document as example...`),
    TE.chainFirst((collection) =>
      mongoInsertOne(collection.collection, {
        id: `${Math.floor(Math.random() * 1000)}`,
        firstName: `name${Math.floor(Math.random() * 1000)}`,
        lastName: `surname${Math.floor(Math.random() * 1000)}`,
        dateOfBirth: new Date(),
      })
    ),
    defaultLog.taskEither.info(
      `Document inserted - Press CTRL+C to exit...Waiting...`
    ),
    TE.chainFirst((_) => simulateAsyncPause),
    TE.chain(({ client }) => waitForExit(client)),
    TE.orElse(exitFromProcess)
  )();

main().catch(console.error).finally(console.log);
