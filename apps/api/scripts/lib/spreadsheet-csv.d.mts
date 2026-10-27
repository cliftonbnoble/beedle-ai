export const spreadsheetCsvPolicy:'spreadsheet-text-1';
export function spreadsheetCsvCell(value:unknown):string;
export function readCsvRecords(input:string):Array<{line:number;cells:string[]}>;
