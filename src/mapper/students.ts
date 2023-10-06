/* eslint-disable no-console */
import * as A from "fp-ts/lib/Array";
import * as O from "fp-ts/lib/Option";

import { pipe } from "fp-ts/lib/function";
import { ChangeStreamDocument, ChangeStreamInsertDocument } from "mongodb";
import { Student } from "../model/student";

type Transformer = <T = Document>(data: ChangeStreamDocument<T>) => Student[];

const isInsert = <T = Document>(data: ChangeStreamDocument<T>) =>
  "documentKey" in (data as ChangeStreamInsertDocument<T>);
const insertTransformer: Transformer = <T = Document>(
  data: ChangeStreamDocument<T>
) =>
  pipe(
    O.fromNullable(data),
    O.filter(isInsert),
    O.chain(() =>
      O.fromNullable((data as ChangeStreamInsertDocument<T>).fullDocument)
    ),
    O.map((data) => {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const { _id, ...newObj } =
        data as unknown as ChangeStreamInsertDocument<T>;
      return newObj;
    }),
    O.fold(
      () => [],
      (data) => {
        const student = data as unknown as Student;
        // eslint-disable-next-line functional/immutable-data
        student.timestamp = data.clusterTime?.toString();
        return A.of(student);
      }
    )
  );
// const deleteTransformer: Transformer = (data) => { ... };
const transformers: Transformer[] = [
  insertTransformer /* , deleteTransformer, ... */,
];

export const transform = <T = Document>(
  messages: ChangeStreamDocument<T>
): Student[] =>
  ([] as Student[]).concat(
    transformers.flatMap((transformer) => transformer(messages))
  );
