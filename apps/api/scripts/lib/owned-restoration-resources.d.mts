export interface RestorationSchemaObject {name:string;type:string;sql:string|null}
// Only pure schema probes are exposed to the scheduled Worker entry point.
export function restorationApplicationObjects(rows:RestorationSchemaObject[]):RestorationSchemaObject[];
export function restorationSchemaFingerprint(rows:RestorationSchemaObject[]):Promise<string>;
