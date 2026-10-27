import { z } from 'zod';
import { RequestConflictError } from './http';
export function storedJson(raw: string): unknown {
    try {
        if (raw.length > 2 * 1024 * 1024)
            throw new Error();
        const value: unknown = JSON.parse(raw);
        return value;
    }
    catch {
        throw new RequestConflictError('Stored JSON is invalid; inspect recovery evidence before retrying');
    }
}
export function validatedStoredJson<Output, Input>(schema: z.ZodType<Output, z.ZodTypeDef, Input>, raw: string): Output {
    const parsed = schema.safeParse(storedJson(raw));
    if (!parsed.success)
        throw new RequestConflictError('Stored payload failed validation; inspect its frozen source or recovery evidence');
    return parsed.data;
}
