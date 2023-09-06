import { AzureEventhubSasFromString } from "@pagopa/fp-ts-kafkajs/dist/lib/KafkaProducerCompact";
import dotenv from "dotenv";
import * as t from "io-ts";
import * as ts from "io-ts-types";

dotenv.config();

export const CosmosDBConfig = t.type({
  CONNECTION_URI: ts.NonEmptyString,
});

export const EventHubConfig = t.type({
  CONNECTION_STRING: AzureEventhubSasFromString,
});

export type ICosmosConfig = t.TypeOf<typeof CosmosDBConfig>;
export type IEventHubConfig = t.TypeOf<typeof EventHubConfig>;

export const MONGO_CONFIG = {
  CONNECTION_URI: process.env.MONGO_CONNECTION_URI,
};

export const EH_CONFIG = {
  CONNECTION_STRING: process.env.EVENT_HUB_CONNECTION_STRING,
};
