export type ReleaseContract = {
    version: number;
    id: string;
    minMigration: number;
    maxMigration: number;
    requiredMigrations: string[];
    searchSchemaVersion: number;
    reviewerPolicyVersion: string;
    objects: {
        name: string;
        type: string;
    }[];
    columns: Record<string, string[]>;
    vectorProfile: string[];
    queries: Record<'objects' | 'columns' | 'migrations' | 'state', string>;
};
export type ReleaseResult = {
    ready: boolean;
    problems: string[];
    capability: string;
    corpusRevision: unknown;
    schemaVersion: unknown;
    searchSchemaVersion: unknown;
    reviewerPolicy: unknown;
};
export function canonical(value: unknown): string;
export function fingerprint(value: unknown): Promise<string>;
export function releaseProfileId(vars: Record<string, unknown>, contract: ReleaseContract): Promise<string>;
export function schemaProblems(contract: ReleaseContract, objects: {
    name: string;
    type: string;
}[], columns: {
    tableName: string;
    columnName: string;
}[], migrations: {
    name: string;
}[]): string[];
export function evaluateReleaseState(contract: ReleaseContract, state: Record<string, unknown>, vars: Record<string, unknown>): ReleaseResult;
