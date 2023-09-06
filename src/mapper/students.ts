import * as A from "fp-ts/lib/Array";
import * as O from "fp-ts/lib/Option";

import { pipe } from "fp-ts/lib/function";
import { ChangeStreamDocument, ChangeStreamInsertDocument } from "mongodb";
import { Student } from "../model/student";

type Transformer = <T = Document>(data: ChangeStreamDocument<T>) => Student[];

const isHandled = <T = Document>(data: ChangeStreamDocument<T>) =>
  "documentKey" in (data as ChangeStreamInsertDocument<T>);
const insertTransformer: Transformer = <T = Document>(
  data: ChangeStreamDocument<T>
) =>
  pipe(
    O.fromNullable(data),
    O.filter(isHandled),
    O.chain(() =>
      O.fromNullable((data as ChangeStreamInsertDocument<T>).fullDocument)
    ),
    O.map((newObject) => {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const { _id, ...newObj } =
        newObject as unknown as ChangeStreamInsertDocument<T>;
      return newObj;
    }),
    O.fold(
      () => [],
      (newObject) => A.of(newObject as unknown as Student)
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
