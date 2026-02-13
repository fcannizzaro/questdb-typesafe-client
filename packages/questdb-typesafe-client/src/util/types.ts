/** Make a type more readable in IDE tooltips */
export type Prettify<T> = { [K in keyof T]: T[K] } & {};

/** Extract string keys from an object type */
export type StringKeyOf<T> = Extract<keyof T, string>;
