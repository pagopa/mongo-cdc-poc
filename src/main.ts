import {
  AzureEventhubSasFromString,
  KafkaProducerCompact,
  fromSas,
  sendMessages,
} from "@pagopa/fp-ts-kafkajs/dist/lib/KafkaProducerCompact";
import dotenv from "dotenv";
import * as E from "fp-ts/Either";
import * as C from "fp-ts/lib/Console";
import * as IO from "fp-ts/lib/IO";
import * as TE from "fp-ts/lib/TaskEither";
import { pipe } from "fp-ts/lib/function";
import { withLogger } from "logging-ts/lib/IO";
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

const databaseName = "mongo-cdc-poc-mongodb";
const collectionName = "students";

const getCosmosConnectionURI = (): TE.TaskEither<Error, string> =>
  pipe(
    CosmosDBConfig.decode(MONGO_CONFIG),
    E.map((config) => config.CONNECTION_URI),
    TE.fromEither,
    TE.mapLeft(
      (errors) =>
        new Error(`Error during decoding Cosmos ConnectionURI - ${errors}`)
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
    TE.mapLeft(
      (errors) => new Error(`Error during decoding Event Hub SAS - ${errors}`)
    )
  );

const simulateAsyncPause: TE.TaskEither<Error, void> = TE.tryCatch(
  () =>
    new Promise<void>((resolve) => {
      setTimeout(() => resolve(), 1000);
    }),
  () => new Error("An error occurred")
);

export const log = withLogger(IO.io)(C.log);

const waitForExit = (client: MongoClient): TE.TaskEither<Error, void> =>
  TE.tryCatch(
    async () => {
      process.stdin.resume();
      process.on("SIGINT", () => {
        disconnectMongo(client);
        process.exit(0);
      });
    },
    (reason) => new Error(`Failed to set exit handler: ${String(reason)}`)
  );

const exitFromProcess = (): TE.TaskEither<Error, void> =>
  pipe(
    log(() => "Application failed"),
    process.exit(1)
  );

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
    TE.bind("client", (connectionUri) => mongoConnect(connectionUri)),
    TE.bind("db", ({ client }) => getMongoDb(client, databaseName)),
    TE.bind("collection", ({ db }) => getMongoCollection(db, collectionName)),
    TE.bind("messagingClient", () => getEventHubProducer()),
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
    TE.chainFirst((_) => simulateAsyncPause),
    TE.chainFirst((collection) =>
      mongoInsertOne(collection.collection, {
        id: `${Math.floor(Math.random() * 1000)}`,
        firstName: `name${Math.floor(Math.random() * 1000)}`,
        lastName: `surname${Math.floor(Math.random() * 1000)}`,
        dateOfBirth: new Date(),
      })
    ),
    TE.chainFirst((_) => simulateAsyncPause),
    TE.chain(({ client }) => waitForExit(client)),
    TE.orElse(exitFromProcess)
  )();

main()
  .catch((err) => log(() => "Error: " + String(err)))
  .finally(() => log(() => "Exiting..."));
