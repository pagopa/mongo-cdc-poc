/* eslint-disable no-console */
import * as http from "http";

import {
  AzureEventhubSasFromString,
  KafkaProducerCompact,
  fromSas,
  sendMessages
} from "@pagopa/fp-ts-kafkajs/dist/lib/KafkaProducerCompact";
import { defaultLog, useWinston, withConsole } from "@pagopa/winston-ts";
import dotenv from "dotenv";
import express, { Request, Response } from "express";
import * as E from "fp-ts/Either";
import * as TE from "fp-ts/lib/TaskEither";
import { pipe } from "fp-ts/lib/function";
import { ChangeStreamDocument, Db, MongoClient } from "mongodb";
import { CosmosDBConfig, EH_CONFIG, MONGO_CONFIG } from "./config/config";
import { transform } from "./mapper/students";
import { Student } from "./model/student";

import { Id } from "./model/resumeToken";
import {
  disconnectMongo,
  findLastToken,
  getMongoCollection,
  getMongoDb,
  mongoConnect,
  mongoInsertOne,
  setMongoListenerOnEventChange,
  watchMongoCollection
} from "./mongo/mongoOperation";

dotenv.config();
useWinston(withConsole());

const databaseName = "mongo-cdc-poc-mongodb";
const collectionName = "students";
export const resumeToken = "resumeToken";

const getCosmosConnectionURI = (): TE.TaskEither<Error, string> =>
  pipe(
    CosmosDBConfig.decode(MONGO_CONFIG),
    E.map(config => config.CONNECTION_URI),
    TE.fromEither,
    TE.mapLeft(errors =>
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
    E.map(sas => fromSas<Student>(sas)),
    TE.fromEither,
    TE.mapLeft(errors =>
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
    new Promise<void>(resolve => {
      setTimeout(() => resolve(), 1000);
    }),
  error =>
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
    reason =>
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
  (messagingClient: KafkaProducerCompact<Student>, db: Db) =>
  <T = Document>(change: ChangeStreamDocument<T>): void =>
    void pipe(
      change,
      transform,
      students => sendMessages(messagingClient)(students)(),
      mongoInsertOne(db.collection(resumeToken), {
        // eslint-disable-next-line no-underscore-dangle
        resumeToken: JSON.stringify((change._id as Id)._data)
      })
    );

const app = express();

app.get(
  "/",
  (req: Request, res: Response) => async () =>
    // const config: ConsumerConfig = {
    //   groupId: "",
    // };
    // const kafkaConfig: KafkaConfig = {
    //   brokers: [],
    // };
    // const consumer = new Kafka(kafkaConfig).consumer(config);
    // await consumer.connect();
    // // eslint-disable-next-line functional/no-let
    // let messages: string[] = [];
    // await consumer.run({
    //   eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
    //     console.log({
    //       value: message.value?.toString(),
    //       topic,
    //       partition,
    //     });
    //     messages = [...messages, message.value?.toString()];
    //   },
    // });
    // await consumer.disconnect();
    res.status(200).send("Messages Read")
);
const main = () =>
  pipe(
    TE.Do,
    getCosmosConnectionURI,
    defaultLog.taskEither.info("Connecting to mongo..."),
    TE.bind("client", connectionUri => mongoConnect(connectionUri)),
    TE.bind("db", ({ client }) => getMongoDb(client, databaseName)),
    TE.bind("collection", ({ db }) => getMongoCollection(db, collectionName)),
    defaultLog.taskEither.info(
      `Connected to DB ${databaseName} - Working on collection ${collectionName}`
    ),
    defaultLog.taskEither.info(
      "Trying to connect to the event hub instance..."
    ),
    TE.bind("messagingClient", () => getEventHubProducer()),
    defaultLog.taskEither.info("Connected to event hub"),
    defaultLog.taskEither.info(
      `Trying to watch the collection ${collectionName}`
    ),
    TE.chainFirst(({ db, collection, messagingClient }) =>
      pipe(
        findLastToken(db.collection("resumeToken")),
        TE.chain(resumeToken =>
          watchMongoCollection(
            collection,
            // eslint-disable-next-line no-underscore-dangle
            resumeToken.resumeToken
          )
        ),
        TE.chain(watcher =>
          setMongoListenerOnEventChange(
            watcher,
            sendMessageEventHub(messagingClient, db)
          )
        )
      )
    ),
    defaultLog.taskEither.info(`Watching the collection ${collectionName}`),
    TE.chainFirst(_ => simulateAsyncPause),
    TE.chainFirst(_ => simulateAsyncPause),
    TE.chain(({ client }) => waitForExit(client)),
    TE.orElse(exitFromProcess)
  )();

main()
  .catch(console.error)
  .finally(() => {
    const server = http.createServer(app).listen(8080, () => {
      console.log("Listening on port %d", 8080);
    });
    server.on("close", () => {
      app.emit("server:stop");
    });
  });
